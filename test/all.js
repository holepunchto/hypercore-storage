// This runner is auto-generated by Brittle

runTests()

async function runTests () {
  const test = (await import('brittle')).default

  test.pause()

  await import('./atomic.js')
  await import('./basic.js')
  await import('./core.js')
  await import('./snapshot.js')
  await import('./streams.js')

  test.resume()
}
