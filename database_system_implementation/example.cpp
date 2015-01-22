R JOIN S on R.attr1=S.attr2
if(R.Count == R(attr1).unique)///attr1 is key in R
{
	return R.blocks + S.blocks + S.count/(bfr of joined Table)
} 

else if(S.Count == S(attr2).unique)////attr2 is key in S
{
	return R.blocks + S.blocks + R.count/(bfr of joined Table)
}
else
{
	return R.blocks + S.blocks + ((S.count/S.unique) * (R.count/R.unique))*min(S.unique,R.unique)/2
}
