// Independent PR #66953 P1 experiment; production sources are unmodified.
#include "ray/object_manager/object_manager.h"
#include "ray/object_manager_rpc_client/object_manager_client.h"
#include "ray/gcs_rpc_client/fake_gcs_client.h"
#include "ray/object_manager/fake_object_directory.h"
#include "ray/object_manager/plasma/fake_plasma_client.h"
#include "gtest/gtest.h"
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <future>
#include <iostream>
#include <map>
#include <mutex>
#include <stdexcept>
#include <thread>

namespace ray {
namespace {
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;
constexpr int64_t kFiniteDeadlineMs = 3000;
constexpr auto kGuard = 10s;
void Require(bool value, const std::string &why) {
  if (!value) throw std::runtime_error(why);
}

struct Trace {
  const Clock::time_point start = Clock::now();
  std::mutex mutex;
  std::condition_variable cv;
  std::map<std::string, int> counts;
  std::map<std::string, Clock::time_point> times;
  std::map<std::string, Status> statuses;
  int sequence = 0;
  void Record(const std::string &event, const std::string &detail = "") {
    std::lock_guard<std::mutex> lock(mutex);
    ++counts[event];
    times[event] = Clock::now();
    std::cout << "P1 seq=" << ++sequence << " us="
              << std::chrono::duration_cast<std::chrono::microseconds>(
                     times[event] - start).count()
              << " event=" << event << " " << detail << std::endl;
    cv.notify_all();
  }
  int Count(const std::string &event) {
    std::lock_guard<std::mutex> lock(mutex);
    return counts[event];
  }
  Clock::time_point Time(const std::string &event) {
    std::lock_guard<std::mutex> lock(mutex);
    return times.at(event);
  }
  void Wait(const std::string &event) {
    std::unique_lock<std::mutex> lock(mutex);
    Require(cv.wait_for(lock, kGuard, [&] { return counts[event] > 0; }),
            "guard expired waiting for " + event);
  }
  void Completion(const std::string &label, const Status &status) {
    {
      std::lock_guard<std::mutex> lock(mutex);
      statuses[label] = status;
    }
    Record(label + ".client_callback", "status=" + status.ToString());
  }
  Status GetStatus(const std::string &label) {
    std::lock_guard<std::mutex> lock(mutex);
    return statuses.at(label);
  }
};

class HeldReplyService : public rpc::ObjectManagerService::Service {
 public:
  HeldReplyService(std::shared_ptr<Trace> trace, std::string a, std::string b)
      : trace_(std::move(trace)), a_(std::move(a)), b_(std::move(b)) {}
  grpc::Status Push(grpc::ServerContext *ctx,
                    const rpc::PushRequest *request,
                    rpc::PushReply *) override {
    const std::string label = request->object_id() == a_ ? "A" :
                              request->object_id() == b_ ? "B" : "UNKNOWN";
    auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
        ctx->deadline() - std::chrono::system_clock::now()).count();
    trace_->Record(label + ".server_received",
                   "chunk=" + std::to_string(request->chunk_index()) +
                   " bytes=" + std::to_string(request->data().size()) +
                   " deadline_remaining_ms=" + std::to_string(remaining));
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait(lock, [&] { return release_all_ || released_[label]; });
    }
    trace_->Record(label + ".server_return");
    return grpc::Status::OK;
  }
  void Release(const std::string &label) {
    trace_->Record(label + ".explicit_release");
    std::lock_guard<std::mutex> lock(mutex_);
    released_[label] = true;
    cv_.notify_all();
  }
  void ReleaseAll() {
    std::lock_guard<std::mutex> lock(mutex_);
    release_all_ = true;
    cv_.notify_all();
  }
 private:
  std::shared_ptr<Trace> trace_;
  std::string a_, b_;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::map<std::string, bool> released_;
  bool release_all_ = false;
};

// A transparent observer: the base implementation starts the real RPC.
// Only a real gRPC completion may invoke the supplied production callback.
class ObservedClient : public rpc::ObjectManagerClient {
 public:
  ObservedClient(const std::string &address, int port, rpc::ClientCallManager &calls,
                 std::shared_ptr<Trace> trace, std::string a)
      : ObjectManagerClient(address, port, calls), main_(calls.GetMainService()),
        trace_(std::move(trace)),
        a_(std::move(a)) {}
  void Push(const rpc::PushRequest &request,
            const rpc::ClientCallback<rpc::PushReply> &callback) override {
    const std::string label = request.object_id() == a_ ? "A" : "B";
    trace_->Record(label + ".real_client_dispatch");
    rpc::ObjectManagerClient::Push(
        request, [trace = trace_, label, callback, main = &main_](const Status &status,
                                                 rpc::PushReply &&reply) {
          trace->Completion(label, status);
          callback(status, std::move(reply));
          trace->Record(label + ".production_callback_forwarded");
          // Both posts originate in this same main-service handler. With its
          // single-thread scheduler they enter the same FIFO private queue:
          // production OnChunkComplete first, then this observation-only marker.
          main->post([trace, label] {
            trace->Record(label + ".main_completion_barrier");
          }, "P1.CompletionBarrier");
        });
  }
 private:
  instrumented_io_context &main_;
  std::shared_ptr<Trace> trace_;
  std::string a_;
};
}  // namespace

// Uses ObjectManager's EXISTING friend seam, as its original test fixture does.
// This is a separate executable; the original test source is not linked.
class ObjectManagerTest : public ::testing::Test {
 protected:
  template <typename F>
  auto OnMain(F fn) {
    using R = decltype(fn());
    auto task = std::make_shared<std::packaged_task<R()>>(std::move(fn));
    auto future = task->get_future();
    main_io_.post([task] { (*task)(); }, "P1.MainBarrier");
    Require(future.wait_for(kGuard) == std::future_status::ready,
            "main barrier exceeded failure guard");
    return future.get();
  }
  struct Accounting { int64_t flight, remaining, queued; };
  Accounting Snapshot(const std::string &name) {
    return OnMain([this, name] {
      const auto &pm = *manager_->push_manager_;
      Accounting a{pm.NumChunksInFlight(), pm.NumChunksRemaining(),
                   pm.NumPushRequestsWithChunksToSend()};
      trace_->Record(name, "flight=" + std::to_string(a.flight) +
                              " remaining=" + std::to_string(a.remaining) +
                              " queued=" + std::to_string(a.queued));
      return a;
    });
  }
  void ExpectAccounting(const Accounting &a, int64_t flight,
                        int64_t remaining, int64_t queued) {
    Require(a.flight == flight && a.remaining == remaining && a.queued == queued,
            "unexpected PushManager accounting");
  }
  void AddObject(const ObjectID &id, uint8_t value) {
    store_->objects_in_plasma_.emplace(
        id, std::make_pair(std::vector<uint8_t>{value}, std::vector<uint8_t>{}));
    ObjectInfo info;
    info.object_id = id;
    info.data_size = 1;
    info.metadata_size = 0;
    info.owner_node_id = local_;
    info.owner_worker_id = WorkerID::FromRandom();
    info.owner_ip_address = "127.0.0.1";
    info.owner_port = 9999;
    manager_->HandleObjectAdded(info);
  }
  void RunExperiment() {
    const char *setting = std::getenv("P1_PUSH_DEADLINE_MS");
    Require(setting != nullptr, "P1_PUSH_DEADLINE_MS is required");
    const int64_t deadline = std::stoll(setting);
    Require(deadline == -1 || deadline == kFiniteDeadlineMs, "invalid test arm");
    RayConfig::instance().initialize(
        "{\"object_manager_push_rpc_timeout_ms\":" + std::to_string(deadline) + "}");
    trace_->Record("configuration", "deadline_ms=" + std::to_string(deadline) +
                    " capacity=1 chunk_bytes=1 observation_ms=4000");

    service_ = std::make_unique<HeldReplyService>(trace_, a_.Binary(), b_.Binary());
    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(service_.get());
    builder.SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::MIN_POLLERS, 2);
    builder.SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::MAX_POLLERS, 4);
    server_ = builder.BuildAndStart();
    Require(server_ && port > 0, "remote server failed to start");
    trace_->Record("remote_server_started", "port=" + std::to_string(port));

    gcs_ = std::make_unique<gcs::FakeGcsClient>();
    directory_ = std::make_unique<FakeObjectDirectory>();
    store_ = std::make_shared<plasma::FakePlasmaClient>();
    rpc::GcsNodeAddressAndLiveness address;
    address.set_node_id(remote_.Binary());
    address.set_node_manager_address("127.0.0.1");
    address.set_object_manager_port(port);
    gcs_->fake_node_accessor->SetNodeAddressAndLiveness(remote_, address);

    ObjectManagerConfig config{};
    config.object_manager_address = "127.0.0.1";
    config.object_manager_port = 0;
    // This P1 fixture has no real Plasma runner and does not test Pull retry.
    // Keep its unrelated store-polling timer outside the bounded experiment.
    config.timer_freq_ms = 600000;
    config.pull_timeout_ms = 10000;
    config.object_chunk_size = 1;
    config.max_bytes_in_flight = 1;
    config.store_socket_name = "p1_fake_local_store";
    config.push_timeout_ms = 10000;
    config.rpc_service_threads_number = 1;
    config.object_store_memory = 1024;
    config.huge_pages = false;
    manager_ = std::make_unique<ObjectManager>(
        main_io_, local_, config, *gcs_, directory_.get(),
        [](const ObjectID &, int64_t, const std::string &,
           std::function<void(const Status &)>) {},
        [](const ObjectID &) { return std::string{}; },
        [](const ObjectID &) -> std::unique_ptr<RayObject> { return nullptr; },
        store_, nullptr,
        [this](const std::string &host, int p, rpc::ClientCallManager &calls) {
          trace_->Record("real_client_created");
          return std::make_shared<ObservedClient>(host, p, calls, trace_, a_.Binary());
        }, rpc_io_);
    AddObject(a_, 'A');
    AddObject(b_, 'B');
    main_thread_ = std::thread([this] { main_io_.run(); });
    rpc_thread_ = std::thread([this] { rpc_io_.run(); });

    OnMain([this] {
      manager_->Push(a_, remote_);
      trace_->Record("A.admitted");
    });
    trace_->Wait("A.server_received");
    ExpectAccounting(Snapshot("A.occupies_capacity"), 1, 1, 0);
    OnMain([this] {
      Require(trace_->Count("A.client_callback") == 0, "A completed before B queued");
      manager_->Push(b_, remote_);
      trace_->Record("B.queued_before_A_completion");
      Require(manager_->push_manager_->NumChunksInFlight() == 1, "capacity not full");
      Require(manager_->push_manager_->NumPushRequestsWithChunksToSend() == 1,
              "B not actually queued in real PushManager");
      Require(trace_->Count("B.real_client_dispatch") == 0, "B bypassed capacity");
    });
    ExpectAccounting(Snapshot("B.blocked"), 1, 2, 1);

    // A common observation endpoint, not a sleep to arrange ordering. Positive
    // ordering is established by real events above; both arms hold A until here.
    const auto checkpoint = trace_->Time("A.server_received") + 4000ms;
    OnMain([this, checkpoint] {
      checkpoint_ = std::make_unique<boost::asio::steady_timer>(main_io_, checkpoint);
      checkpoint_->async_wait([trace = trace_](const boost::system::error_code &ec) {
        if (!ec) trace->Record("observation_checkpoint");
      });
    });
    trace_->Wait("observation_checkpoint");
    Require(trace_->Count("A.explicit_release") == 0, "A was released early");
    Require(trace_->Count("A.server_return") == 0, "server completed A early");
    if (deadline != -1) {
      Require(trace_->Count("A.client_callback") == 1, "missing/duplicate A callback");
      Require(trace_->GetStatus("A").IsTimedOut(), "A was not a real deadline failure");
      Require(trace_->Count("B.server_received") == 1, "B did not start after timeout");
      Require(trace_->Time("B.queued_before_A_completion") <
                  trace_->Time("A.client_callback"), "B was queued too late");
      Require(trace_->Time("A.client_callback") < trace_->Time("B.real_client_dispatch"),
              "B was not dispatched after A completion");
      ExpectAccounting(Snapshot("checkpoint.finite"), 1, 1, 0);
    } else {
      Require(trace_->Count("A.client_callback") == 0, "disabled A completed early");
      Require(trace_->Count("B.real_client_dispatch") == 0, "disabled B started early");
      Require(trace_->Count("B.server_received") == 0, "disabled B reached server");
      ExpectAccounting(Snapshot("checkpoint.disabled"), 1, 2, 1);
    }

    service_->Release("A");
    trace_->Wait("B.server_received");
    service_->Release("B");
    trace_->Wait("A.production_callback_forwarded");
    trace_->Wait("B.production_callback_forwarded");
    trace_->Wait("A.main_completion_barrier");
    trace_->Wait("B.main_completion_barrier");
    trace_->Wait("A.server_return");
    trace_->Wait("B.server_return");
    // Cross-thread post alone is not a barrier: Asio can queue it ahead of the
    // current handler's private queue. The same-handler markers above establish
    // that both production OnChunkComplete calls have actually run before readout.
    ExpectAccounting(Snapshot("all_completions_drained"), 0, 0, 0);
    Require(trace_->GetStatus("B").ok(), "B failed");
    if (deadline == -1) Require(trace_->GetStatus("A").ok(), "disabled A failed");
    Cleanup();
    for (const auto &label : {std::string("A"), std::string("B")}) {
      Require(trace_->Count(label + ".real_client_dispatch") == 1, "duplicate dispatch");
      Require(trace_->Count(label + ".client_callback") == 1, "duplicate completion");
      Require(trace_->Count(label + ".production_callback_forwarded") == 1,
              "duplicate production callback forwarding");
      Require(trace_->Count(label + ".main_completion_barrier") == 1,
              "missing/duplicate completion barrier");
    }
    Require(trace_->Count("real_client_created") == 1, "unexpected client factory");
    trace_->Record("PASS", "callbacks_A=1 callbacks_B=1 flight=0 remaining=0 queued=0");
  }
  void Cleanup() {
    if (cleaned_) return;
    cleaned_ = true;
    if (service_) service_->ReleaseAll();
    if (server_) {
      server_->Shutdown(std::chrono::system_clock::now() + kGuard);
      server_->Wait();
    }
    main_io_.stop();
    rpc_io_.stop();
    if (main_thread_.joinable()) main_thread_.join();
    if (rpc_thread_.joinable()) rpc_thread_.join();
    manager_.reset();  // Also shuts down and joins ClientCallManager's CQ threads.
    server_.reset();
    trace_->Record("cleanup_complete", "application_threads_joined=true");
  }
  ~ObjectManagerTest() override { Cleanup(); }
  std::shared_ptr<Trace> trace_ = std::make_shared<Trace>();
  ObjectID a_ = ObjectID::FromRandom(), b_ = ObjectID::FromRandom();
  NodeID local_ = NodeID::FromRandom(), remote_ = NodeID::FromRandom();
  instrumented_io_context main_io_{false, true}, rpc_io_{false, true};
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
      main_work_{main_io_.get_executor()}, rpc_work_{rpc_io_.get_executor()};
  std::thread main_thread_, rpc_thread_;
  std::unique_ptr<boost::asio::steady_timer> checkpoint_;
  std::unique_ptr<gcs::FakeGcsClient> gcs_;
  std::unique_ptr<FakeObjectDirectory> directory_;
  std::shared_ptr<plasma::FakePlasmaClient> store_;
  std::unique_ptr<ObjectManager> manager_;
  std::unique_ptr<HeldReplyService> service_;
  std::unique_ptr<grpc::Server> server_;
  bool cleaned_ = false;
};

TEST_F(ObjectManagerTest, P1RealDeadlineRestoresQueuedPushCapacity) {
  EXPECT_NO_THROW(RunExperiment());
}
}  // namespace ray
