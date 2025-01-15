from ...utils.env import env_with_fallback

BASE_HASHBOARD_URI = (
    env_with_fallback("HASHQUERY_API_BASE_URI", "HASHBOARD_CLI_BASE_URI")
    or "https://hashboard.com"
)
