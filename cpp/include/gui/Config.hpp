// ============================================================================
// Configuration: Frame rate limit
// ============================================================================
constexpr float TARGET_FPS = 30.0; // Adjust this to control CPU usage
constexpr float FRAME_TIME = 1.0 / TARGET_FPS;
constexpr bool HIGH_FPS_ON_EVENTS = false;       // true: auto boost FPS on mouse move, false: fixed low FPS
constexpr bool VSYNC_ENABLE = TARGET_FPS > 60.0; // Enable VSync only when FPS > 60
