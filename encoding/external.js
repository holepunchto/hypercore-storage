function headLegacyMap(m) {
  return {
    version: 2, // version bumped from 1
    cores: m.allocated.cores,
    datas: m.allocated.datas,
    groups: 0,
    seed: m.seed,
    defaultDiscoveryKey: m.defaultDiscoveryKey
  }
}

module.exports = {
  headLegacyMap
}
