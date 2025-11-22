// ============================================================================
// Configuration: Frame rate limit
// ============================================================================
constexpr double TARGET_FPS = 10.0; // Adjust this to control CPU usage
constexpr double FRAME_TIME = 1.0 / TARGET_FPS;
constexpr bool HIGH_FPS_ON_EVENTS = false;       // true: auto boost FPS on mouse move, false: fixed low FPS
constexpr bool VSYNC_ENABLE = TARGET_FPS > 60.0; // Enable VSync only when FPS > 60
