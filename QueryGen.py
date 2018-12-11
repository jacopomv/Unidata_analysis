class Query:
    def __init__(self, name, body):
        self.name = name
        self.body = body

q1 = Query("query", {
  "query": {
    "bool": {
    "must": [
      {
        "match": {
          "gateway": "7276FF002E0616C0"

        }
      },
      {
        "match": {
          "dev_eui": "0018B24000000186"
        }
      },
      {
        "match": {
          "size": "23"
        }
      }
    ]
    }
  }
})
q2 = Query("Spec_GW", {
  "query":{
    "match":{
      "gateway":"7276FF002E0616C0"
    }
  }
})
q3 = Query("Match All", {
  "query": {
    "match_all": {}
  }
})
