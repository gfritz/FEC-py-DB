#!/usr/bin/env python2.7.3
# -*- coding: UTF-8 -*-

import json
import marshal, types
from copy import deepcopy

def _default(self, obj):
    return getattr(obj.__class__, "to_json", _default.default)(obj)
_default.default = json.JSONEncoder().default # save unmodified default
json.JSONEncoder.default = _default # replacement

class FECSchema:
	def __init__(self):
		self.schema = {
			"test-database" : {
				"table-one" : {
					"triggers" : {},
					"column-one" : {
						"field-type" : "", 
						"triggers" : {
							"before-update" : lambda f,x: f(x),
							"after-update" : lambda f,x: f(x),
							"level" : {"before-udate" : lambda f,x: f(x)}
						},
						"size" : 0
					}
				}
			}
		}

	def serializing_walk_schema(self,cur_layer, to_be_serialized_object):
		if hasattr(cur_layer,'__call__'):
			return marshal.dumps(cur_layer.func_code)
		else:
			if isinstance(cur_layer, dict):
				for key,val in cur_layer.iteritems():
					to_be_serialized_object[key] = self.serializing_walk_schema(val,deepcopy(val))
				return to_be_serialized_object	
			else:
				#Base types return the dumped
				return marshal.dumps(cur_layer)

	def unserialize_walk_schema(self,cur_layer, serialized_object,load_trigger=False):
		if isinstance(cur_layer,dict):
			for key,val in cur_layer.iteritems():
				if key == "triggers":
					serialized_object[key] = self.unserialize_walk_schema(val,deepcopy(val),load_trigger=True)
				else:
					serialized_object[key] = self.unserialize_walk_schema(val,deepcopy(val),load_trigger)

			return serialized_object
		else:
			if load_trigger:
				code= marshal.loads(serialized_object)
				func = types.FunctionType(code, globals(), serialized_object)
				return func
			else:
				return marshal.loads(serialized_object)


	def serialize(self):
		#Walk the schema and serialize triggers
		tmp = deepcopy(self.schema)
		serialized = self.serializing_walk_schema(tmp,{})
		return serialized

	def unserialize(self, string):
		self.schema = self.unserialize_walk_schema(string,{})
		return self #chain me if you like





def test():
	test = FECSchema()
	print test.serialize()
	test.unserialize(test.serialize())
	print test.schema
	trig = test.schema["test-database"]["table-one"]["column-one"]["triggers"]['level']["before-udate"]
	print trig(sum,[1,4])

if __name__=="__main__":
	test()
