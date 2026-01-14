### Determine which Interface is used
import platform



# Function: Determine which OS is used
def detect_os_profile(override: str = None) -> str:
    """
    Returns: 'linux' | 'macos' | 'windows'
    Uses OS_OVERRIDE if set; otherwise auto-detects via platform.system().
    """
    profiles = ["linux", "windows", "macos"]
    if override is not None:
        key = override.strip().lower()
        if key not in profiles:
            raise ValueError(f"OS_OVERRIDE must be one of {list(profiles.keys())}, got: {override!r}")
        return key

    sysname = platform.system().lower()
    if "linux" in sysname:
        return "linux"
    if "darwin" in sysname:
        return "macos"
    if "windows" in sysname:
        return "windows"
    # fallback
    return "linux"
