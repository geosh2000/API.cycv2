<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Fams extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->database();
  }

  public function activeFams_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $asesor = $this->uri->segment(3);

      if($qDep = $this->db->query("SELECT hc_dep FROM asesores_plazas WHERE id=getVacanteAsesor($asesor, CURDATE())")){
        $depData = $qDep->row_array();
        $dep = $depData['hc_dep'];
      }else{
        errResponse('Error al obtener el departamento del asesor '.$asesor, REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
      }

      $this->db->select("a.*, asistira, shown")
              ->from("config_famDisplay a")
              ->join("config_famAsist b", " a.id=b.Evento AND b.asesor=$asesor", "left")
              ->like( 'departamentos', "|$dep|", 'both' )
              ->where( array("activo" => 1) )
              ->where( "NOW() BETWEEN ", "date_start_display AND date_limit_display", FALSE );

      if( $q = $this->db->get() ){

        okResponse( 'Información Obtenida', 'data', $q->result_array(), $this, 'rows', $q->num_rows() );

      }else{

        errResponse('Error al obtener data personal del asesor '.$asesor, REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }


      return true;

    });

    jsonPrint( $result );

  }

  public function markRead_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      if( $this->db->query("INSERT INTO config_famAsist (asesor, asistira, shown, Evento) VALUES (".$data['asesor'].", 0, 1, ".$data['evento'].") ON DUPLICATE KEY UPDATE shown = 1") ){

        okResponse( 'Información actualizada', 'data', true, $this );

      }else{

        errResponse('Error al marcar evento como leido', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }


      return true;

    });

    jsonPrint( $result );

  }

  public function subscribeFam_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      if( $this->db->query("INSERT INTO config_famAsist (asesor, asistira, shown, Evento) VALUES (".$data['asesor'].", 1, 1, ".$data['evento'].") ON DUPLICATE KEY UPDATE asistira = 1") ){

        okResponse( 'Información actualizada', 'data', true, $this );

      }else{

        errResponse('Error al marcar evento como leido', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }


      return true;

    });

    jsonPrint( $result );

  }

  public function unSubscribeFam_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      if( $this->db->query("INSERT INTO config_famAsist (asesor, asistira, shown, Evento) VALUES (".$data['asesor'].", 0, 1, ".$data['evento'].") ON DUPLICATE KEY UPDATE asistira = 0") ){

        okResponse( 'Información actualizada', 'data', true, $this );

      }else{

        errResponse('Error al marcar evento como leido', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }


      return true;

    });

    jsonPrint( $result );

  }

  public function displayActive_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      if( $q = $this->db->query("SELECT * FROM config_famDisplay WHERE NOW() < date_limit_display AND activo = 1") ){

        if( $p = $this->db->query("SELECT * FROM config_famDisplay WHERE NOW() >= date_limit_display OR activo = 0") ){

          if( $c = $this->db->query("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'config_famDisplay'") ){

            okResponse( 'Información obtenida', 'data', array( 'actual' => $q->result_array(), 'past' => $p->result_array(), 'cols' => $c->result_array()), $this );

          }else{

            errResponse('Error al marcar evento como leido', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

          }

        }else{

          errResponse('Error al marcar evento como leido', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

        }

      }else{

        errResponse('Error al marcar evento como leido', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }


      return true;

    });

    jsonPrint( $result );

  }

  public function editFam_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      $this->db->where( array( 'id' => $data['id'] ) )
              ->set($data['update']);

      if( $this->db->update("config_famDisplay") ){

        okResponse( 'Información Actualizada', 'data', true, $this );

      }else{

        errResponse('Error al actualizar Evento', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }


      return true;

    });

    jsonPrint( $result );

  }

  public function newFam_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      $this->db->set($data);

      if( $this->db->insert("config_famDisplay") ){

        okResponse( 'Información Guardada', 'data', true, $this );

      }else{

        errResponse('Error al guardar Evento', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }


      return true;

    });

    jsonPrint( $result );

  }

}
