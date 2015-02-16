# HybridAlgorithm
Hadoop MapReduce Job
Crystal ball to predict events that may happen once a certain event happened

Mapper Pseudo code

 class Mapper
   method Initialize
	H = new AssociativeArray()	
	
   method Map(docid a; doc d)
	for all term w in doc d do
	     for all term u in neighbor(w) do
		H{pair(w; u)} = H{pair(w; u)} + 1
   method close
	for all pair p in H do
	     Emit(pair p; count H{pair p})
  

Reducer Pseudo code

 class Reducer	
      method Initialize
        marginal = 0;
        H = new AssociativeArray ()  

   method Reduce(pair(w; u); counts[c1;c2; …])
	if (currentTerm == null) then
		currentTerm = w;
	else if (currentTerm != w) then
		for all term u in H do
			H{u} = H{u} / marginal
			Emit(term currentTerm, stripe H)
	          marginal = 0;
                  H.clear()	        
	          currentTerm = w;
	
	for all count c in counts[c1;c2; …] do
		H{u} = H{u} + c
		marginal = marginal + c
	
     method Close 
	for all term u in H do
	      H{u} = H{u} / marginal
	      Emit(term currentTerm, stripe H)

