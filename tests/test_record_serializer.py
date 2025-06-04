from record_serializer import RecordSerializer
import constants

serializer = RecordSerializer()

def test_serialize_deserialize_int():
    record = {"age": 42}
    columns = ["age"]
    data = serializer.serialize(record)
    result = serializer.deserialize(data, columns)
    assert result == record

def test_serialize_deserialize_float():
    record = {"height": 5.9}
    columns = ["height"]
    data = serializer.serialize(record)
    result = serializer.deserialize(data, columns)
    assert abs(result["height"] - 5.9) < 1e-6

def test_serialize_deserialize_str():
    record = {"name": "Gosha"}
    columns = ["name"]
    data = serializer.serialize(record)
    result = serializer.deserialize(data, columns)
    assert result == record

def test_serialize_deserialize_bool():
    record = {"married": True}
    columns = ["married"]
    data = serializer.serialize(record)
    result = serializer.deserialize(data, columns)
    # bool serialized as int 1
    assert result == {"married": 1}

def test_serialize_deserialize_null():
    record = {"nickname": None}
    columns = ["nickname"]
    data = serializer.serialize(record)
    result = serializer.deserialize(data, columns)
    assert result == record

def test_end_to_end_serialization():
    record = {
        "name": "Gosha",
        "age": 30,
        "height": 5.9,
        "married": True,
        "nickname": None
    }
    columns = ["name", "age", "height", "married", "nickname"]
    data = serializer.serialize(record)
    result = serializer.deserialize(data, columns)
    assert result["name"] == "Gosha"
    assert result["age"] == 30
    assert abs(result["height"] - 5.9) < 1e-6
    assert result["married"] == 1
    assert result["nickname"] is None
