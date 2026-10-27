// Temporary independent regression for PR #67417. Shared unchanged by parent and HEAD.
TEST_F(GcsNodeManagerTest, TestRestoredHeadRegistrationReplayHasNoDeathSideEffects) {
  RayConfig::instance().initialize(R"({"enable_ray_event": true})");
  auto publisher = std::make_unique<pubsub::FakePublisher>();
  auto *recorded_publisher = publisher.get();
  gcs_publisher_ = std::make_unique<pubsub::GcsPublisher>(std::move(publisher));
  auto make_manager = [&]() {
    return std::make_unique<gcs::GcsNodeManager>(
        gcs_publisher_.get(), gcs_table_storage_.get(), *io_context_,
        client_pool_.get(), ClusterID::Nil(), *fake_ray_event_recorder_,
        "recovery-replay-test", observability_publisher_.get(), clock_);
  };
  auto drain = [&]() {
    io_context_->restart();
    while (io_context_->poll() > 0) {}
  };
  auto head = GenNodeInfo(1);
  head->set_is_head_node(true);
  head->set_start_time_ms(clock_.NowUnixMillis());
  const auto id = NodeID::FromBinary(head->node_id());
  rpc::RegisterNodeRequest original_request;
  original_request.mutable_node_info()->CopyFrom(*head);
  const auto original_bytes = original_request.SerializeAsString();

  // Use the real registration handler to produce the stored ALIVE record.
  // The reply sink observes server completion; no client receives an acknowledgement.
  {
    auto original_manager = make_manager();
    rpc::RegisterNodeReply reply;
    int completed = 0;
    original_manager->HandleRegisterNode(
        original_request, &reply,
        [&](Status status, std::function<void()>, std::function<void()>) {
          EXPECT_TRUE(status.ok());
          ++completed;
        });
    drain();
    ASSERT_EQ(completed, 1);
    ASSERT_TRUE(original_manager->IsNodeAlive(id));
    ASSERT_FALSE(original_manager->IsNodeDead(id));
  }

  // Keep only the external-store analogue; create fresh, empty manager caches.
  recorded_publisher->publish_calls.clear();
  fake_ray_event_recorder_->FlushBuffer();
  auto restored_manager = make_manager();
  ASSERT_TRUE(restored_manager->GetAllAliveNodes().empty());
  ASSERT_TRUE(restored_manager->GetAllDeadNodes().empty());
  int added = 0;
  std::vector<rpc::GcsNodeInfo> removed;
  restored_manager->AddNodeAddedListener(
      [&](std::shared_ptr<const rpc::GcsNodeInfo>) { ++added; }, *io_context_);
  restored_manager->AddNodeRemovedListener(
      [&](std::shared_ptr<const rpc::GcsNodeInfo> node) { removed.push_back(*node); },
      *io_context_);
  io_context_->restart();
  auto init_data = LoadInitData();
  ASSERT_EQ(init_data.Nodes().size(), 1u);
  ASSERT_EQ(init_data.Nodes().at(id).SerializeAsString(), head->SerializeAsString());
  restored_manager->Initialize(init_data);
  drain();
  ASSERT_TRUE(restored_manager->IsNodeAlive(id));
  ASSERT_FALSE(restored_manager->IsNodeDead(id));
  ASSERT_EQ(added, 0);
  ASSERT_TRUE(removed.empty());
  ASSERT_TRUE(recorded_publisher->publish_calls.empty());
  RAY_LOG(INFO) << "RECOVERY_REPLAY phase=restored alive=1 dead=0 stored=ALIVE";

  rpc::RegisterNodeRequest replay_request = original_request;
  ASSERT_EQ(replay_request.SerializeAsString(), original_bytes);
  rpc::RegisterNodeReply replay_reply;
  int replies = 0;
  restored_manager->HandleRegisterNode(
      replay_request, &replay_reply,
      [&](Status status, std::function<void()>, std::function<void()>) {
        EXPECT_TRUE(status.ok());
        ++replies;
      });
  RAY_LOG(INFO) << "RECOVERY_REPLAY phase=handler_return alive="
                << restored_manager->IsNodeAlive(id) << " dead="
                << restored_manager->IsNodeDead(id);
  drain();
  ASSERT_EQ(replies, 1);

  // Read the table through the same production loading path, separately from caches.
  io_context_->restart();
  auto final_data = LoadInitData();
  drain();
  ASSERT_EQ(final_data.Nodes().size(), 1u);
  const auto &stored = final_data.Nodes().at(id);
  int dead_info = 0;
  int dead_liveness = 0;
  int alive_info = 0;
  int alive_liveness = 0;
  for (const auto &message : recorded_publisher->publish_calls) {
    ASSERT_EQ(message.key_id(), id.Binary());
    if (message.channel_type() == rpc::ChannelType::GCS_NODE_INFO_CHANNEL) {
      ASSERT_EQ(message.node_info_message().node_id(), id.Binary());
      if (message.node_info_message().state() == rpc::GcsNodeInfo::DEAD) ++dead_info;
      else ++alive_info;
    } else if (message.channel_type() ==
               rpc::ChannelType::GCS_NODE_ADDRESS_AND_LIVENESS_CHANNEL) {
      ASSERT_EQ(message.node_address_and_liveness_message().node_id(), id.Binary());
      if (message.node_address_and_liveness_message().state() == rpc::GcsNodeInfo::DEAD)
        ++dead_liveness;
      else ++alive_liveness;
    } else {
      ADD_FAILURE() << "Unexpected node publication channel";
    }
  }
  RAY_LOG(INFO) << "RECOVERY_REPLAY phase=complete alive="
                << restored_manager->IsNodeAlive(id) << " dead="
                << restored_manager->IsNodeDead(id) << " stored="
                << rpc::GcsNodeInfo::GcsNodeState_Name(stored.state())
                << " removed=" << removed.size() << " added=" << added
                << " dead_info=" << dead_info << " dead_liveness=" << dead_liveness
                << " alive_info=" << alive_info << " alive_liveness=" << alive_liveness;
  EXPECT_EQ(stored.SerializeAsString(), head->SerializeAsString());
  EXPECT_EQ(restored_manager->GetAllAliveNodes().size(), 1u);
  EXPECT_TRUE(restored_manager->IsNodeAlive(id));
  EXPECT_TRUE(restored_manager->GetAllDeadNodes().empty());
  EXPECT_FALSE(restored_manager->IsNodeDead(id));
  EXPECT_TRUE(removed.empty());
  EXPECT_EQ(added, 0);
  EXPECT_EQ(dead_info, 0);
  EXPECT_EQ(dead_liveness, 0);
  // Do not impose exactly-once ALIVE notification semantics on registration retries.
  EXPECT_EQ(alive_info, 1);
  EXPECT_EQ(alive_liveness, 1);
}
