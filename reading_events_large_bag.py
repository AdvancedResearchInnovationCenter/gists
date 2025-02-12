import rosbag
import numpy as np
from pathlib import Path
from tqdm.auto import tqdm
import dask.array as da
# Open ROSbag

path = Path('/path/')
rosbag_file = 'bag_name.bag'
bag = rosbag.Bag(path / rosbag_file)


events = []
event_topic = '/capture_node/events'

for topic, msg, t in tqdm(
    bag.read_messages(topics=[event_topic]),
    total=sum([bag.get_message_count(top) for top in [event_topic]]),
    desc='parsing events',
    unit='msg'
):
    if topic == event_topic:
        ev_array = []
        for e in msg.events:
            event = [e.x, e.y, e.ts.to_nsec(), e.polarity]
            ev_array.append(event)
        events.append(da.array(ev_array))
        
da_event = da.vstack(events)
da_event.to_hdf5(path / 'events.h5', 'events')