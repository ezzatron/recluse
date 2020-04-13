module.exports = {
  normalizeSpec,
}

function normalizeSpec (spec) {
  const {
    aggregates = {},
    initialize = noop,
    integrations = {},
    name,
    processes = {},
    projections = {},
  } = spec

  if (!name) throw new Error('Invalid app name')

  return {
    aggregates: mapObjectValues(normalizeAggregate, aggregates),
    initialize,
    integrations: mapObjectValues(normalizeIntegration, integrations),
    name,
    processes: mapObjectValues(normalizeProcess, processes),
    projections: mapObjectValues(normalizeProjection, projections),
  }
}

function normalizeAggregate (aggregate) {
  const {
    applyEvent = noop,
    commandTypes = [],
    createInitialState = noop,
    eventTypes = [],
    handleCommand = noop,
    routeCommand = noop,
  } = aggregate

  return {
    applyEvent,
    commandTypes,
    createInitialState,
    eventTypes,
    handleCommand,
    routeCommand,
  }
}

function normalizeIntegration (integration) {
  const {
    commandTypes = [],
    eventTypes = [],
    handleCommand = noop,
  } = integration

  return {
    commandTypes,
    eventTypes,
    handleCommand,
  }
}

function normalizeProcess (process) {
  const {
    commandTypes = [],
    createInitialState = noop,
    eventTypes = [],
    handleEvent = noop,
    routeEvent = noop,
  } = process

  return {
    commandTypes,
    createInitialState,
    eventTypes,
    handleEvent,
    routeEvent,
  }
}

function normalizeProjection (projection) {
  const {
    applyEvent = noop,
  } = projection

  return {
    applyEvent,
  }
}

function noop () {}

function mapObjectValues (fn, object) {
  const result = {}

  for (const key in object) {
    result[key] = fn(object[key], key)
  }

  return result
}
