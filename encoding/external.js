function headLegacyMap(m) {
  return {
    version: 2,
    allocated: {
      cores: m.allocated.cores,
      datas: m.allocated.datas,
      groups: 0
    },
    seed: m.seed,
    defaultDiscoveryKey: m.defaultDiscoveryKey
  }
}

module.exports = {
  headLegacyMap
}
