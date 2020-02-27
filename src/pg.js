module.exports = {
  acquireSessionLock,
}

async function acquireSessionLock (context, logger, client, namespace, id = 0) {
  await context.do(async () => {
    logger.debug(`Acquiring session lock for ${namespace}.${id}`)
    await client.query('SELECT pg_advisory_lock($1, $2)', [namespace, id])
    logger.debug(`Acquired session lock for ${namespace}.${id}`)

    await context.onceDone(() => client.query('SELECT pg_advisory_unlock($1, $2)', [namespace, id]))
  })
}
