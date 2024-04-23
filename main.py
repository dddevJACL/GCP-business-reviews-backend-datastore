from flask import Flask, request
from google.cloud import datastore

BUSINESSES = "businesses"
BUSINESSES_ATTRIBUTES = ["owner_id", "name", "street_address", "city", "state", "zip_code"]
REVIEWS = "reviews"
POST_ERROR = {"Error" : "The request body is missing at least one of the required attributes"}

app = Flask(__name__)
client = datastore.Client()

@app.route('/')
def index():
    """
    TODO
    """
    return 'Please navigate to /' + BUSINESSES + "or /" + REVIEWS + "to use this API"

@app.route('/' + BUSINESSES, methods=['GET'])
def get_businesses():
    """
    TODO
    """
    datastore_query = client.query(kind=BUSINESSES)
    query_results = list(datastore_query.fetch())
    query_results = add_id_attribute(query_results)
    return query_results

def add_id_attribute(datastore_entities_list, more_than_one=True):
    """
    Takes a list of datastore entities, and adds an 'id' attribute to them
    based on the datastore key.id value. If more_than_one is True, this function
    returns a list, else it returns the lone entity
    """
    for entity in datastore_entities_list:
        entity["id"] = entity.key.id
    if more_than_one:
        return datastore_entities_list
    return datastore_entities_list[0]


@app.route('/' + BUSINESSES + '/<int:id>', methods=['GET'])
def get_business(id):
    """
    TODO
    """
    requested_business = get_entity_by_id(id, BUSINESSES)
    if requested_business is None:
        return ({"Error": "No business with this business_id exists"}, 404)
    return requested_business


def get_entity_by_id(id, entity_type):
    """
    TODO
    """
    entity_key = client.key(entity_type, id)
    entity = client.get(key=entity_key)
    if entity is None:
        return None
    entity = add_id_attribute([entity], False)
    return entity

@app.route('/' + BUSINESSES, methods=['POST'])
def post_businesses():
    """
    TODO
    """
    body_content = request.get_json()
    valid_request = validate_post(body_content, BUSINESSES_ATTRIBUTES)

    if valid_request:
        new_business = datastore.Entity(key=client.key(BUSINESSES))
        new_business.update(create_post_dict(body_content, BUSINESSES_ATTRIBUTES))
        client.put(new_business)
        new_business = add_id_attribute([new_business], False)
        return (new_business, 201)
    
    return (POST_ERROR, 400)


def validate_post(json_request, arr_of_keys):
    """
    Checks to see if required attributes are included in body of Post request.
    Expects to recieve and array of attribute keys to test on the json body.
    Returns True if the json request is valid, or False otherwise.
    """
    for key in arr_of_keys:
        if key not in json_request:
            return False
    return True

def create_post_dict(json_request, arr_of_keys):
    """
    Creates and returns dictionary for use in a datastore.Entity update call, 
    usingThe given json_request and array of key names.
    """
    # WILL THIS FUNCTION CAUSE A PROBLEM???? CAN IT BE A DICT, OR DOES IT NEED TO BE WRITTEN
    # IN UPDATE FUNCTION CALL???
    update_dict = dict()
    for key in arr_of_keys:
        update_dict[key] = json_request[key]
    return update_dict

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)