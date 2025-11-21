## Layout

```
=======================================================================================================
Tasks:                                          | Panel:                                              |
    > Settings [synced/syncing/writing]         |   Selected task content displays here               |
    > SystemInfo [/live]                        |                                                     |
    > DataBase                                  |                                                     |
    > Features                                  |                                                     |
    > Factors                                   |                                                     |
...                                             |                                                     |
                                                |======================================================
                                                |Terminal:                                            |
icon:                                           |   Command output and logs                           |
Network|FPS|                                    |                                                     |
=======================================================================================================
```

### Task-Based System
Each business module = One task file

**IGuiTask** interface:
- `GetName()` - Task name in list
- `GetStatus()` - Current status (synced, syncing, live, etc.)
- `DrawPanel()` - Draw task content in right panel
- `OnExpand()` - Called when task is selected
- `OnCollapse()` - Called when task is deselected

**TaskWithCoroutines** base class (optional):
- Automatic coroutine lifetime management
- `AddCoroutine(handle)` - Register a managed coroutine
- `CancelAllCoroutines()` - Stop all registered coroutines
- Auto-cancellation on destruction

### Current Tasks

#### Regular Tasks (non-coroutine)
1. **SettingsTask** (`SettingsTask.cpp`)
   - Config management with auto-sync
   - Status: "initializing" → "syncing" → "synced"
   - Features: Auto-sync, debounced save, file change detection

2. **SystemInfoTask** (`SystemInfoTask.cpp`)
   - Hardware and OS information
   - Status: "live" (always monitoring)
   - Features: CPU/RAM info, real-time monitoring with ImPlot

#### Coroutine-Based Tasks
3. **CoroCrawler** (`coro/crawler/CoroCrawler.cpp`)
   - Web scraping with concurrency
   - Status: "crawling" / "idle"
   - Pattern: Multiple concurrent coroutines
   - Features: Parallel HTTP requests, result aggregation

### File Organization

```
cpp/
├── include/gui/
│   ├── GuiTask.hpp              # Task interface & base classes
│   ├── GuiState.hpp             # GUI state (includes NetworkState)
│   └── coro/                    # Coroutine infrastructure
│       ├── CoroManager.hpp      # Coroutine manager + handle
│       ├── CoroUtils.hpp        # Utilities (RunTimer, CoroSleep, etc.)
│       ├── CoroNetwork.hpp      # Network monitoring coroutine
│       └── CoroCrawler.hpp      # Web crawler coroutine
│
├── src/gui/
│   ├── Gui.cpp                  # Main GUI loop and layout
│   ├── GuiState.cpp             # GuiState + Corotine datastructs
│   ├── GuiTask.cpp              # TaskWithCoroutines
│   ├── IconBar.cpp              # Icon bar
│   ├── SettingsTask.cpp         # Settings task
│   ├── SystemInfoTask.cpp       # System info task
│   └── coro/                    # Coroutine implementations
│       ├── CoroManager.cpp      # CoroManager + CoroutineHandle impl
│       ├── network/
│       │   └── CoroNetwork.cpp  # ASIO async TCP connect
│       └── crawler/
│           └── CoroCrawler.cpp  # Beast async HTTP Web crawler
```

