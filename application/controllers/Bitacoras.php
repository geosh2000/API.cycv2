<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Bitacoras extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('jwt');

    $this->load->database();

  }

  public function addEntry_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      $insert = array(
                      'asesor' => $data['asesor'],
                      'actividades' => nl2br($data['comments'])
                    );

      if($this->db->set($insert)
                  ->set('date_created', 'NOW()', FALSE)
                  ->insert('bitacoras_supervisores')){
                    $result = array('status' => true, 'msg' => 'Guardado correctamente');
                  }else{
                    $result = array('status' => false, 'msg' => $this->db->error());
                  }

      return $result;

    });

    jsonPrint( $result );

  }

}
