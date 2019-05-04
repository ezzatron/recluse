const {mapObjectValues} = require('./object.js')

module.exports = {
  normalizeSpec,
}

function normalizeSpec (spec) {
  const {
    name,

    aggregates = {},
    integrations = {},
    processes = {},
    projections = {},
  } = spec

  if (!name) throw new Error('Invalid app name')

  return {
    name,

    aggregates: mapObjectValues(normalizeAggregate, aggregates),
    integrations: mapObjectValues(normalizeIntegration, integrations),
    processes: mapObjectValues(normalizeProcess, processes),
    projections: mapObjectValues(normalizeProjection, projections),
  }
}

function normalizeAggregate (aggregate) {
  const {
    commandTypes = [],
    eventTypes = [],
    routeCommand = noop,
    createInitialState = noop,
    handleCommand = noop,
    applyEvent = noop,
  } = aggregate

  return {
    commandTypes,
    eventTypes,
    routeCommand,
    createInitialState,
    handleCommand,
    applyEvent,
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
    eventTypes = [],
    commandTypes = [],
    routeEvent = noop,
    createInitialState = noop,
    handleEvent = noop,
  } = process

  return {
    eventTypes,
    commandTypes,
    routeEvent,
    createInitialState,
    handleEvent,
  }
}

function normalizeProjection (projection) {
  const {
    apply = noop,
  } = projection

  return {
    apply,
  }
}

function noop () {}
