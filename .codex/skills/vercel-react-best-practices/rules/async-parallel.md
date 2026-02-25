# async-parallel

Why it matters:
- Parallelizing independent async work reduces total response time and removes avoidable waterfalls.

Incorrect:
```ts
const user = await getUser(id);
const posts = await getPosts(id);
```

Correct:
```ts
const [user, posts] = await Promise.all([getUser(id), getPosts(id)]);
```
