<?php

function prettyPrint( $arreglo ){

  echo "<pre>";
  print_r($arreglo);
  echo "</pre>";

}

function jsonPrint( $arreglo ){

  echo json_encode( $arreglo );

}

function onDuplicateUpdate( $class, $params, $table ){

  foreach($params as $key => $value){
    @$result.="`$key`=".$class->db->escape($value).", ";
  }

  $class->db->set($params);
  $insertQuery = $class->db->get_compiled_insert($table);

  $onDup = "$insertQuery ON DUPLICATE KEY UPDATE ".substr($result,0,-2);

  if ($class->db->simple_query($onDup)){
          $result = array( "status" => true, "msg" => "Query Completed");
          return $result;
  }else{
          $result = array( "status" => false, "msg" => $class->db->error());
          return $result;
  }

}

function validateToken( $token_uri, $usn, $function ){

  $token = JWT::validateToken( $token_uri, $usn, 'cAlbertyCome' );

  if( !$token['status'] ){
      $result = array(
                    "status"    => false,
                    "msg"       => $token['msg']
                  );
  }else{

     $result = $function();

  }

  return $result;

}




//
// require( APPPATH.'/libraries/REST_Controller.php');
// use Restserver\Libraries\REST_Controller;


?>
