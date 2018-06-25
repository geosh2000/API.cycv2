<?php

  function fact($num)
  {
      $res = 1;
      for ($n = $num; $n >= 1; $n--)
          $res = $res*$n;
          //echo "( fact($num)=$res)";
      return $res;
  }

  function utilization($intensity, $agents){

      if($agents!=0){
      	$utilization=$intensity/$agents;

      	if($utilization>1){$utilization=1;}
      	if($utilization<0){$utilization=0;}

      	//echo "( $intensity/$agents=$utilization)";
      	return $utilization;
      }

  }


  function top($intensity, $agents){

  	$top=pow($intensity,$agents)/fact($agents);
  	//echo "( $intensity^$agents^/fact($agents)=$top)";
  	return $top;
  }



  function erlangBR($intensity, $agents){
  	$k=0;
  	$max=$agents-1;
  	$answer=0;
  	while($k<=$max){
  		$answer=$answer+(pow($intensity,$k)/fact($k));
  	$k++;
  	}

  	return $answer;

  }

  function erlangC($intensity, $agents){
  	$erlangC=(top($intensity,$agents)) / ((top($intensity,$agents)) + ((1-utilization($intensity,$agents)) * erlangBR($intensity,$agents)));

  	if($erlangC>1){$erlangC=1;}
  	if($erlangC<0){$erlangC=0;}

  	return $erlangC;
  }

  function servicelevel($intensity, $agents,$target,$duration){

  	$servicelevel= 1 - (erlangC($intensity, $agents) * exp(-($agents-$intensity) * $target / $duration));

  	if($servicelevel>1){$servicelevel=1;}
  	if($servicelevel<0){$servicelevel=0;}

  	return $servicelevel;

  }

  function agentno($intensity, $target,$duration,$servreq){


  	$minagents=intval($intensity);

  	$agents=$minagents;

  	while(servicelevel($intensity, $agents,$target,$duration)<$servreq){

  	$agents++;
  	}


  	return $agents;

  }

  ?>
