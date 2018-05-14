import os
from flask import Flask, jsonify, make_response, abort, url_for, request
import json

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False
app.config.update(dict(
  PREFERRED_URL_SCHEME = 'https'
))

mongoURI = "mongodb://"+os.getenv("MONGOUSER")+":"+os.getenv("MONGOPASS")+"@"+os.getenv("MONGOSERVER")+"/"+os.getenv("MONGOPATH")

def getMongoClient(freeAndOpen=False):
    from pymongo import MongoClient
    return MongoClient(mongoURI)

def getDB(dbname):
    from pymongo import MongoClient
    client = MongoClient(mongoURI)
    return client[dbname]

bis = getDB("bis")
nvcs = bis["NVCS"]

def make_public_unit(unit):
    new_unit = {}
    for field in unit:
        if field == '_id':
            new_unit['uri'] = url_for('get_unit', unit_id=unit['_id'], _scheme="https", _external=True)
        else:
            new_unit[field] = unit[field]
    return new_unit

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

@app.errorhandler(422)
def uprocessable_query(error):
    return make_response(jsonify({'error': 'Query parameters, q and fields, must be structured in valid JSON syntax'}), 422)

@app.route("/bis", methods=["GET"])
def get_documentation():
    doc = {}
    doc["Description"] = "Welcome to the completely experimental and not at all stable proto-API for the Biogeographic Information System. This is very R&D, so don't build anything live on it."
    doc["End Points"] = {}
    doc["End Points"]["bis/api/v0.1/usnvc"] = "API methods for interfacing with the U.S. National Vegetation Classification"
    return jsonify(doc)

@app.route("/bis/api/v0.1/usnvc", methods=["GET"])
def get_methods():
    methods = {}
    methods["units"] = {"Description":"Retrieve USNVC unit data",
        "Search Parameters":[
            {"q":{"Description":"Must be supplied as a valid JSON string that executes a find operation against the database in raw form.","Link":"https://docs.mongodb.com/manual/tutorial/query-documents/"}},
            {"fields":{"Description":"Must be supplied as a valid JSON string that sets the fields to either return or suppress in the output.","Link":"https://docs.mongodb.com/manual/tutorial/project-fields-from-query-results/"}}
    ],
        "Identifiers":"Include the element_global_id after units/ to retrieve a single unit"
    }
    methods["hierarchy"] = {"Description":"Retrieve the full NVCS hierarchy in a JSON hierarchical form as one nested document. The output includes the title and a generated URI to retrieve a given unit."}
    return jsonify(methods)

@app.route("/bis/api/v0.1/usnvc/units", methods=["GET"])
def get_units(skip=0,limit=10):
    if request.args.get("skip") is not None:
        skip = int(request.args.get("skip"))
    else:
        skip = 0

    if request.args.get("limit") is not None:
        limit = int(request.args.get("limit"))
    else:
        limit = 10
    
    queryParams = []
    
    if request.args.get("q") is not None:
        import json
        try:
            query = json.loads(request.args.get("q"))
        except:
            abort(422)
    else:
        query = {}
        
    if request.args.get("fields") is not None:
        import json
        try:
            fields = json.loads(request.args.get("fields"))
        except:
            abort(422)
    else:
        fields = None

    results = {"total":nvcs.find(query).count()}
    
    if results["total"] > limit:
        results["nextlink"] = {"rel":"next","url":url_for('get_units', limit=limit, skip=skip+limit, q=query, fields=fields, _scheme="https", _external=True)}
    if skip > 0:
        results["prevlink"] = {"rel":"previous","url":url_for('get_units', limit=limit, skip=skip-limit, q=query, fields=fields, _scheme="https", _external=True)}
    
    results["units"] = [make_public_unit(unit) for unit in nvcs.find(query,fields).skip(skip).limit(limit)]
    return jsonify(results)

@app.route("/bis/api/v0.1/usnvc/units/<unit_id>", methods=["GET"])
def get_unit(unit_id):
    if request.args.get("fields") is not None:
        import json
        try:
            fields = json.loads(request.args.get("fields"))
        except:
            abort(422)
    else:
        fields = None

    unit = nvcs.find_one({"_id":int(unit_id)},fields)
    if unit is None:
        abort(404)
    return jsonify({"unit":make_public_unit(unit)})

@app.route("/bis/api/v0.1/usnvc/hierarchy", methods=["GET"])
def get_hierarchy():
    from collections import OrderedDict
    nvcsData = {}
    for unit in nvcs.find({},{"title":1}):
        nvcsData[int(unit["_id"])] = unit["title"] 
    
    simpleList = []
    for unit in nvcs.find({"_id":{"$ne":0}},{"title":1,"parent":1}).sort("Hierarchy.unitsort",1):
        simpleList.append([nvcsData[int(unit["parent"])],unit["title"]])

    sortedList = []
    for classUnit in nvcs.find({"parent":0},{"title":1}).sort("Hierarchy.unitsort",1):
        sortedList.append(classUnit["title"])

    units = {}
    for parent, child in simpleList:
        parent_dict = units.setdefault(parent, {})
        child_dict = units.setdefault(child, make_public_unit({"_id":list(nvcsData.keys())[list(nvcsData.values()).index(child)]}))
        if child not in parent_dict:
            parent_dict[child] = child_dict
    
    return jsonify(OrderedDict((k, units["US National Vegetation Classification"][k]) for k in sortedList))

app.run(host=os.getenv("IP","0.0.0.0"),port=int(os.getenv("PORT",8080)))