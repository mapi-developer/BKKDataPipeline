import json
from unittest.mock import patch, MagicMock
import producer

@patch('producer.requests.get')
@patch('producer.producer')
@patch('producer.gtfs_realtime_pb2.FeedMessage')
def test_fetch_and_produce(mock_feed_class, mock_kafka, mock_get):
    mock_response = MagicMock()
    mock_response.content = b'fake_protobuf_data'
    mock_get.return_value = mock_response

    mock_feed_instance = MagicMock()
    mock_feed_class.return_value = mock_feed_instance
    
    mock_entity = MagicMock()
    mock_entity.HasField.return_value = True
    mock_entity.vehicle.vehicle.id = "123"
    mock_entity.vehicle.trip.trip_id = "trip_1"
    mock_entity.vehicle.trip.route_id = "route_1"
    mock_entity.vehicle.position.latitude = 47.49791
    mock_entity.vehicle.position.longitude = 19.04023
    mock_entity.vehicle.timestamp = 1712000000
    
    mock_feed_instance.entity = [mock_entity]

    producer.fetch_and_produce()

    mock_get.assert_called_once_with(producer.BKK_URL)
    
    assert mock_kafka.produce.call_count == 1
    
    call_obj = mock_kafka.produce.call_args
    actual_topic = call_obj.args[0] if call_obj.args else call_obj.kwargs.get('topic')
    assert actual_topic == producer.KAFKA_TOPIC
    
    assert call_obj.kwargs.get('key') == "123"
    
    produced_data = json.loads(call_obj.kwargs['value'])
    assert produced_data['vehicle_id'] == "123"
    assert produced_data['route_id'] == "route_1"
    
    mock_kafka.flush.assert_called_once()
    