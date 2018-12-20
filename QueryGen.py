class Query:
    def __init__(self, name, body):
        self.name = name
        self.body = body

q1 = Query("query_GWToSF", {
   "query" : {
      "constant_score" : {
         "filter" : {
            "bool" : {
              "must" : [
                 { "term" : {"gateway" : "7276ff002e061491"}},
                 { "term" : {"datr" : "sf12bw125"}}
              ],
           }
         }
      }
   }
})
q2 = Query("Spec_GW", {
    "query" : {
        "constant_score" : {
            "filter" : {
                "term" : {
                    "gateway" : "7276ff002e061491"
                }
            }
        }
    }
})
q3 = Query("Match All", {
  "query": {
    "match_all": {}
  }
})


#Top GWs:
#58A0CBEFFE014E4C - first - 58a0cbeffe014e4c
#7276FF002E0503C8 - second - 7276ff002e0503c8
#1C497BEFFECAB36D - third - 1c497beffecab36d
#7276FF002E0616FD - fourth - 7276ff002e0616fd
#7276FF002E0616C0 - fifth - 7276ff002e0616c0
#7276FF002E061491 - sixth - 7276ff002e061491
