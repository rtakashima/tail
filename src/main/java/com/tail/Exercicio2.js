var map = function() { emit(this.marca, this.valor) };

var reduce = function(key,values){ return Array.sum(values) };
/*
db.produto.mapReduce(
  map,
  reduce,
  {
    out: "result"
  }
);
*/