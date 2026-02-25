# bundle-barrel-imports

Why it matters:
- Barrel imports can pull in more code than needed and increase client bundle size.

Incorrect:
```ts
import { Button } from '@/components';
```

Correct:
```ts
import { Button } from '@/components/button';
```
