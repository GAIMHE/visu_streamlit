# Code Review Checklist

## Correctness
- Validate behavior changes against requirements.
- Check edge cases and error handling.

## Security
- Validate input handling and output escaping.
- Check secrets handling and auth boundaries.

## Performance
- Watch for N+1 queries and redundant network calls.
- Evaluate algorithmic complexity in hot paths.

## Testing
- Ensure tests cover changed behavior.
- Verify negative and boundary scenarios.
