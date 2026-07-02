
## [0.1.16] - 2026-07-02

### Bugfixes

- [cli] Handle tasks with a TaskCompletion but no TaskStarted (82d1a773ddfcca8f1fe827e0e1e57d0398b134e6)

## [0.1.15] - 2026-06-26

### Bugfixes

- [cli] Amend glob pattern construction (665e33c47cdda381843c6e45a5a5eab7da74a81f)

## [0.1.14] - 2026-06-19

### New Features

- [pysdk] Expose sql_query on job context (52dbaa1421bae9a846b80c7eef0dea9debde4721)
- Drop support for pyspark (27b9fc4651bb364373bb3b70765159eea77e7494)
- Support feature flags in the config file (0551708421c1657788d5c9da7fbb70fc607494df)
- Add include_paths for SQL models below the root (5b5a37f2fce82e2efb4ee32c2df4bc19e4bc3222)

### Bugfixes

- [cli] Align checkout -b tip and help with real behavior (30e936a8dd80533d9f81d4c5580f0c53076610a8)

## [0.1.13] - 2026-06-15

### New Features

- [pysdk] Expose job error messages (a61f6c27f5955161cbb4ce20fa283d2cee30971b)

### Bugfixes

- Respect explicit job user filters (74f52b24df062c5981fdfe97ff73b90b1fef7bdc)
- [cli] Accept api_endpoint in config set (15c4e42b8363dbadca23d3af093704b4881dad12)
- [cli] Add `version` to generated pyproject in init (ecd8ee0f3dfde8d1da6b711c53f4b095c5a1f05a)

## [0.1.12] - 2026-06-08

### New Features

- [pysdk] Add type contract support (9ef20dfffee16eae10f82b04affd9bd527c0357e)
- Fetch query results directly from the runtimes (927f146f6663d682d9b4af4b856ea9a750285d34)

### Bugfixes

- Enable ureq platform-verifier on all platforms (f2127745fd51ce3e5220cf86f9bc811e662bb44b)
- Use OS platform verifier for ureq HTTPS calls (09621935732d9668d3069d09f35333acb10cb02d)
- [pysdk] Respect timeout when fetching query results over longbow (b612e2a64f04653940d2f93cde60301b1207a471)
- Percent-encode URL path segments (5e5ec8ebca521e5d60aa141c906ea5c16f62f8c1)


## [0.1.10] - 2026-04-07

### New Features

- [pysdk] Change the default timeout for jobs (4dbb98aa2875b662a5bc63925cdaa4a0e9f2bc43)
- Map untyped error responses to ApiError::Other (ac9ab2b6dda8685569a6414cefa2ef07c5f83323)

### Bugfixes

- [cli] Tweak get_info timeout (e1678b8725b8423a1d854845f865027e256073f5)

## [0.1.9] - 2026-03-17

### Bugfixes

- [cli] Honor active branch in table get, query, and run commands (84d9f2f94c839c505225cc406354ddc2b95a3429)

## [0.1.8] - 2026-03-13

### New Features

- [pysdk] Accept a Namespace for filter_by_namespace in get_tables (6e04b3d4e4eb5dff1f3276ea3520c16c8bbc64e5)

### Bugfixes

- [pysdk] Inconsistent raises (2d5f61901dc9c7650149d7a5619b601799d097be)
- Map missing error kinds from API responses (9df535adace5272c35865d793fc1737f740b3009)
- Map UNAUTHORIZED error kind from API responses (8a19443270c51a395b83dad403a405733dc7e755)

## [0.1.7] - 2026-03-06

### New Features

- [pysdk] Allow hooking into `logging` (b3a13bab91ce3c7ea85cee0df5376db4aabe3b45)

### Bugfixes

- [pysdk] Release the GIL during network operations (56cde6f410774a88ffd6511a9ddc21880779183b)

## [0.1.6] - 2026-03-05

### New Features

- [cli] Bauplan init (7dd53354c366e80c6643fe0fd5ab7384e192f4fa)

### Bugfixes

- [cli] Don't print user logs from system tasks (ce8381e197fafe5d3d09c2d3a2ad1c6e5d1aea80)
- Send a "shutdown" request after fetching query results (5276a36569cbc5120ae125e96c2405a5dbff8ae5)

## [0.1.5] - 2026-03-04

### Bugfixes

- [pysdk] Make the client thread-safe (211f7cd2fb93cee81b8b99d7b456498262a0585c)
- [pysdk] Remove `_internal` from `__module__`, fix public stubs (da37f1e7d561d620db0e9fa51f6723af5b22d82e)
- Handle missing context in "forbidden errors" (016d4d830ead788af6e6d252eebf3e71a3ad4051)

## [0.1.4] - 2026-03-02

### Bugfixes

- [cli] In 'config set', create the config file if it doesn't exist (b36ad4b8a59bc775c42059d1066712eba5da0a0e)
- [cli] Add missing committer and parent hashes to commit output (346d1a20fc9503444cf4be29626149160f711e8f)

## [0.1.3] - 2026-03-02

### New Features

- [pysdk] Add __version__ (ceb6237c6de933e1ab94b469604f2dadbfd76b45)
- [cli] Highlight the active branch (62daba8389601b39d6642a5c04001aa628ff6b98)
- [cli] Allow overriding verbosity with RUST_LOG (f1bab4fe2cc9e8ceebc02fc2673b7a66ad9b39d9)

### Bugfixes

- [cli] Fix a runtime error in 'parameter set --type secret' (997a7ec5ad43a294db0bdcd9eb24cc6b307879c9)

## [0.1.2] - 2026-02-26

### Bugfixes

- Correctly set module_version when running jobs (bec31184c185c47c737431e5b6882ecc36998111)
- Allow loading profiles with no API key (c912f2f753a87ab2291bc9973f38ec320059b012)

## [0.1.1] - 2026-02-23

### Bugfixes

- Rewrite scan to use polyglot (c83d000d4b7a5b4dff63a4b179d21d433297f2c5)

