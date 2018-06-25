<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Polls extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->database();
  }

  public function poll_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        $poll = $this->uri->segment(3);

        $query = "SELECT 
                    *
                FROM
                    polls_results
                WHERE 
                    asesor = ".$_GET['usid']."
                    AND poll = '$poll'";
        
      if( $q = $this->db->query( $query ) ){
          
        okResponse( 'Información Obtenida', 'data', $q->row_array(), $this );

      }else{

        errResponse('Error al obtener slots en tiempo real', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }


      return true;

    });

    jsonPrint( $result );

  }
    
  public function save_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        $data = $this->put();
        $poll = $data['poll'];
        $par = $data['data'];

        $insert = $this->db->set($par)
                            ->set(array('poll' => $poll, 'asesor' => $_GET['usid']))
                            ->get_compiled_insert('polls_results');
        $selectors = "";
        
        foreach( $par as $sels => $info ){
            $selectors .= "$sels = '$info' ";
        }
        $query = $insert." ON DUPLICATE KEY UPDATE $selectors";
        
      if( $q = $this->db->query( $query ) ){
          
        okResponse( 'Información Obtenida', 'data', true, $this );

      }else{

        errResponse('Error al obtener slots en tiempo real', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }


      return true;

    });

    jsonPrint( $result );

  }
 
}
