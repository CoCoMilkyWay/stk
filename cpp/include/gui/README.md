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

### Current Tasks
1. **SettingsTask** (`SettingsTask.cpp`)
   - Config management with auto-sync
   - Status: "initializing" → "syncing" (when expanded) → "synced" (when collapsed)
   - Features:
     - Auto-sync on expansion
     - Initial sync on first load
     - Debounced save (200ms)
     - File change detection

2. **SystemInfoTask** (`SystemInfoTask.cpp`)
   - Hardware and OS information
   - Status: "live" (always monitoring)
   - Features:
     - CPU/RAM hardware info
     - OS version, kernel, hostname
     - Real-time CPU usage monitoring with ImPlot
     - Real-time memory usage monitoring with ImPlot
     - Compact and rich display

### File Organization

```
cpp/
├── include/gui/
│   ├── GuiTask.hpp          # Task interface
│   └── GuiState.hpp         # GUI state management
├── src/gui/
│   ├── Gui.cpp              # Main GUI loop and layout
│   ├── SettingsTask.cpp     # Settings module
│   └── SystemInfoTask.cpp   # System info module
```
