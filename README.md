Historian
==========

Historian takes live state and translates it into aggregated streams of historical state readings.

Design
======

The current state for a plane might look something like this:

```json
{
  "state": "FLYING",
  "ap_state": "AUTO_MISSION",
  "flight_state": {
    "position": {
      "lat": 231.33,
      "lng": 1.234,
      "alt": 55
    },
    "velocity": {
      "heading": 33.23,
      "speed": 33.32,
      "alt_rate": 3.23,
      "speed_rate": -1.11
    }
  },
  "flight_target": {
    "type": "gps",
    "params": {
      "position": {
        "lat": 231.33,
        "lng": 1.234,
        "alt": 55
      },
    }
  }
}
```

Current state is a predefined Protobuf structure, but stored as JSON in Consul and in the database (rethink is a key/value db). A cluster-local service named `historian` watches this live state and replicates it to a RethinkDB table using the model described in `COMPONENTS.md` - keyframes, and changes.

State can be "aggregated" or simplified down the line by deleting the interim change records and keeping just the keyframes, and by adjusting the keyframes to set intervals in time by calculating the state at time intervals and recording new keyframe entries.

The state tree would look something like this:

```
└── plane_1
    ├── action_graph
    │   └── state
    └── flight_controller
        └── state
```

The flight controller state would be denoted as `plane_1.flight_controller.state`. This would be the table name in the state database in RethinkDB.

Historian reads out of a configuration table in RethinkDB that says which state entries to record, what fields to ignore / eliminate, what keyframe frequency to use, etc.

Getting data into Historian
========================

Individual components like action_graph and flight_controller should not be expected to talk to Consul directly, nor be queried by the web UI directly. This is where the local service `reporter` comes into play.

The `reporter` is like the newscaster who goes out to report the hurricane, standing on-location in the moment. The `historian` is the history professor sitting in an office somewhere writing down everything the `reporter` is saying.

Historian streams
=================

Historian is configured to manage a set of streams. A stream might have these properties:

 - `id`: based on the hierarchical state identifier, for example, `plane_1.flight_controller.state`
 - `source`: one of a few:
   - `AGGREGATE`: reference other historian streams that contain the data for this stream. When the referenced streams change current state it will also affect the state of this stream.
   - `PUSH`: expect data to be fed into historian from other sources.

Getting data to the viewer
==========================

There are, therefore, two ways to get live state to view it:

 - Contact the reporter directly and ask for a state stream
 - View it from the historian stream of changes.

Inter-dependencies of data
==========================

Imagine we have a sensor in the field that reports temperature. This sensor can be observed by a number of different entities that possess the radios to receive transmissions from this sensor.

We can model the last received transmission from said sensor like so:

```
└── plane_1
    └── sensor_rx
        └── sensor_233
```

Where `sensor_233` contains:

```json
{
  "rx_timestamp": 1475439400,
  "temperature": 30.0
}
```

Now, we want to aggregate this temperature as reported from a number of different sources into a single `historian` stream of state. We can configure a `historian` stream to do this, by setting its type to aggregate and its field object to:

```json
{
  "temperature": {
    "$reference": {
      "stream": "*.sensor_rx.sensor_233",
      "timestamp": "rx_timestamp",
      "field": "temperature",
      "max_rate": 20000
    }
  }
}
```

This will tell historian to take the temperature field from any `sensor_rx.sensor_233` state stream. It will also tell it to use the "rx_timestamp" field as the timestamp, and use the latest from that timestamp for latest state. It also says to use 20 seconds as the maximum rate, which means it will do a couple of things:

 - Use the first reading it gets, don't record additional readings with the same value. (this is default behavior)
 - If it receives another reading within 20 seconds, drop it.

This way we can keep streams of observed data for each receiver, as well as aggregated data from all receivers.
