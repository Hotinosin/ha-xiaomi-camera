# Dual camera and visual feeder

User authorizes forking, modifying and installing Hotinosin/ha-xiaomi-camera on HAOS.

Scope: preserve the working first lens and entity IDs, expose channel 2 through the
same physical SDK session, and attempt video for xiaomi.feeder.iv2002 only. Never
send camera-control properties to the feeder. Existing credentials must survive
deployment; keep the old add-on available for rollback.

## Task 1: Implement and verify

Write failing tests for isolated channel delivery, shared connection lifetime,
second-lens published URLs, channel snapshots and feeder discovery without power
control. Add per-channel media state while keeping one physical session. Publish
one additional original stream per extra channel; keep existing transcodes and
identities. Extend the integration and translations to select that stream. Package
the fork as 2.0.2. Run pytest, ruff and Bun build. Review the whole branch before
publishing.

## Task 2: Publish and install

Push the tested fork, build images with GitHub Actions, back up the existing HAOS
installation, install the fork bridge and integration, and migrate credentials or
hand off Xiaomi login if required. Verify both C300 lenses and feeder video on real
devices. Report any SDK restriction separately from tested software behavior.

Review focus: disconnects must reset all channels; channel inputs must be bounded;
snapshot caches must not mix lenses; feeder must never receive camera power writes;
repository slug changes must not lose credentials or silently rebind entities.
