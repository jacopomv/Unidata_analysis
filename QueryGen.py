class Query:
    def __init__(self, name, body):
        self.name = name
        self.body = body

q1 = Query("query_GWToSF", {
  "query": {
    "bool": {
    "must": [
      {
        "match": {
          "gateway": "7276FF002E0616FD"

        }
      },
      {
        "match": {
          "datr": "SF12BW125"
        }
      },
    ]
    }
  }
})
q2 = Query("Spec_GW", {
  "query":{
    "match":{
      "gateway":"1C497BEFFECAB36D"
    }
  }
})
q3 = Query("Match All", {
  "query": {
    "match_all": {}
  }
})
#Top 2 GWs
#1C497BEFFECAB36D
#7276FF002E0616FD
