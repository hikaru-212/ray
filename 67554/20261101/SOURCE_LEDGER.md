# Source and evidence ledger

## Revision and runtime identity

| Item | Value |
|---|---|
| PR HEAD | `94bc7f96c0df3c967edb8842d6c98d3b658fef42` |
| Controlled baseline | `a0f38c92fcefd3eb02223721fafb54eb3e7156a6` (three commits behind HEAD; not immediate parent) |
| HEAD immediate parent | `a47b7dbada4048c51613ab69d600624c7fb132be` |
| Python | 3.12.3 |
| Ray | `3.0.0.dev0`, compatible prebuilt ARM64 native runtime |
| `ai-dynamo-runtime` | 1.4.0 |
| vLLM | `0.32.0+cpu` |
| Torch | `2.13.0+cpu` |
| Transformers | 5.19.0 |
| ray-haproxy | 2.8.25 |

The exact HEAD tracker was overlaid into a version-compatible Ray 3.0.dev
runtime. No Ray, vLLM, or Torch native source build was used for these runs.

## Installed production artifacts

| Artifact | SHA-256 |
|---|---|
| exact-HEAD `kv_token_tracker.py` | `0ca74ddeaf1be27b31129ec67a9689b72e7866d323e475d6c4876d1091714a5c` |
| Dynamo 1.4.0 `_core.abi3.so` | `131160571ba798adc5293a445e52254ee22ac294ec05d8ce2a52a17db1322177` |
| D1 provenance manifest | `b0498881022cc30ec89374c01fea8a91b548bf04bd79c29635ff97b3ef9660e5` |

Private original runtime paths:

- `/tmp/ray-67554-d1-20261101-0b5XuV/venv/lib/python3.12/site-packages/ray/llm/_internal/serve/routing_policies/kv_aware/kv_token_tracker.py`
- `/tmp/ray-67554-d1-20261101-0b5XuV/venv/lib/python3.12/site-packages/dynamo/_core.abi3.so`
- `/tmp/ray-67554-d1-20261101-0b5XuV/provenance/MANIFEST.txt`

These paths are provenance only; neither venv nor binary is packaged.

## Production method provenance

| Method | First line | Source file SHA-256 | Captured source-block SHA-256 |
|---|---:|---|---|
| `LLMRouter.route` | 183 | `805111735c556da98f33a6fc64e053667d42b89d4b299063895b721c623f9653` | `e0ef82ebff321e17fe19561009541499eb1c785b78b3a4cae32d552dd3e434e0` |
| `KVAwareRouter.choose_replicas` | 60 | `9d640251f3eafcb70f87887a110bcbdb1c8c7c39db376adb26276e92331c840b` | `7911c8e48f56d68972f40597d378f5239a13997ec65d3b54ff3ca16035f714e7` |
| `KVTokenTracker.select_worker` | 412 | `0ca74ddeaf1be27b31129ec67a9689b72e7866d323e475d6c4876d1091714a5c` | `55ef4923a34a14232654de4b23bb574835259582f78e4be347f3d83de58ea99b` |
| `KVTokenTracker._free_abandoned_reservation` | 494 | same tracker hash | `463bc670a761a3c707f88f6e32bafaac30fecc0dae28298bc81c5080a180baeb` |
| `Replica._direct_ingress_asgi` | 3122 | `263e233fb6d96a74ebedcc3dc6da63f2ae2c25be60b1b00b008128e27e13dc30` | `a8778f600375437a7ee4632c71b9427fb6337ffdd6804d6500cffe49bbca708e` |
| `parse_request_timeout_header` | 840 | `fbb72d0e254927708ecdccbf65f02c81127cf75d70f6d68d2596725a1bafc298` | `b8d96a715e7754a5e5aebda514b1bba7dd80b725979a16b9fd818aaf84c75f4b` |
| Dynamo `SelectionService.select_and_reserve` | native | Dynamo binary hash above | native PyO3 method; no Python block hash |

The production path used `DeploymentHandle.choose_replica(_reserve=False)` and
the configured public `ray.serve.llm.request_router.KVAwareRouter`. The active
router inherited the internal production KV-aware implementation and held the
same tracker object as the production LLMRouter; no router fallback was used in
the booking chain.

## Raw evidence directories and hashes

The following absolute paths identify private originals. Only selected summaries
and small derived excerpts are in this public package.

### D2 deployment PASS

Root: `/home/researcher/oss/evidence/ray-67554-d2-minimal-serve-20261101-HnP6Em/evidence`

| File | SHA-256 | Package status |
|---|---|---|
| `summary.json` | `045abf42ba0568d20ce5ca8e570c48d49f38004ea05fa34f6ef9bfd71bf53a82` | omitted; de-identified audit summary included |
| `environment.json` | `2cdf52cedce0887ff008d67a7c9371e38bb10e3b277e55350c2348c12c20cfd0` | omitted; selected facts in ledger |

### D3A HTTP booking PASS

Root: `/home/researcher/oss/evidence/ray-67554-d3a-http-booking-20261101-FFfNoj/evidence`

| File | SHA-256 | Package status |
|---|---|---|
| `summary.json` | `7ab2f705a26827b4d9dc0339675de8ec6fa49fce99d0a7976e2877c74c1ae44b` | omitted; de-identified audit summary included |
| `environment.json` | `c5c211674d13e7fef9d3a029a3e6ab89dcf4162616e2edc526a04442744eb7f2` | omitted |

### D3B normal routing

Root: `/home/researcher/oss/evidence/ray-67554-d3b-cancellation-20261101-fm5aGr/evidence`

| File | SHA-256 | Package status |
|---|---|---|
| `summary.json` | `8d0b9ab8e2dc0a11cfaaece02b6c76a0ee3b15628f5dca82e8d98b9e5896c23c` | omitted (544 KiB repeated samples); audit-derived summary included |
| `events.jsonl` | `a1af9e1ce7c65888593ea80f6bb8fa9c34e021cdfad3680d06ec16838bca745f` | omitted; line-addressed excerpt included |

### D3B-2 initial backpressure

Root: `/home/researcher/oss/evidence/ray-67554-d3b2-native-queue-20261101-zItUJQ/evidence`

| File | SHA-256 | Package status |
|---|---|---|
| `summary.json` | `71df82f0e0a4f253ee190ae3aae66b9bd811162ed075fa0a9eb8c3e2ef52af42` | omitted; de-identified audit summary included |
| `environment.json` | `518a1cc2976daabcb2f01cb70668f9b1381b7896351b3f3ecaec12589332c140` | omitted |
| `events.jsonl` | `2380ebb6d5b74dc277eca9bf02b0ad30de664c8f1e2651eb3e7d8ee691491bf2` | omitted |

### D3B-2 final observation

Root: `/home/researcher/oss/evidence/ray-67554-d3b2-final-observation-20261101-HrASXXue/evidence`

| File | SHA-256 | Package status |
|---|---|---|
| `summary.json` | `6d385e603cfd1524d8c522229c594148942c039c8ce3fe38a2840cbab960ecaa` | copied byte-identically and left uncorrected |
| `environment.json` | `3cde110bc871e45db4c4cf2da9aeb402922f89cbbb3c33dab82a819e81b40b92` | omitted |
| `events.jsonl` | `e4842a5a9f0bd6247552ebaebbca7e48f3ac2207f7ee4873fca80f7d5d726eb0` | omitted; minimal excerpts included |
| `snapshots.jsonl` | `49f8dbe20803b09f476f3becdb9a7280c0bf26b0558013c3a0363f4653c6007a` | omitted (28.6 MB); selected lines included |
| `driver.stderr.log` | `db28da4b5020f01bd14bb7409fb4881982503e02984b56e772ad94bbdd69f102` | omitted; selected lines included |
| `d3b2_adapter.py` | `60178e019687f38cff7c070115e7f6f687edcaf35c404a022bd6c7fbfc92ca47` | omitted; observer predicate cited in correction |

## Controlled comparison archive

Existing package: `../20261030/`

| Existing artifact | SHA-256 |
|---|---|
| `head/summary.json` | `3d92f5602ae3aab98a09d2dae7219a1e3902b89f2dc08636786a68e2b31573fd` |
| `head/timeline.json` | `0be03c7febf197b717b133c2fca22ef2fad7ff29de8a399fb87b9b7c6cf08611` |
| `parent/summary.json` | `6f44f40aae5abd5e3d126bae229aa4a1f7351512db7c382713defda67983cdc8` |
| `parent/timeline.json` | `6fa0557287aba0ffa5dd54b20889a558103e82e9b7d958f977caf9290f5d3b07` |
| `SHA256SUMS` | `1a8c932b40d34f18e27ff6879dd742991cbcce8928ad7e9ed37c40efbbb4d317` |

No file in `../20261030/` was modified during this packaging step.

