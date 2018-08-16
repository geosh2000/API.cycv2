<?php
defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');

class Queuemetrics extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->database();
  }

  public function rtMonitor_post(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $block = $this->post();

      $this->db->select('tipo, json')
              ->select('Last_update')
              ->from('ccexporter.rtMonitor', false)
              ->where_in( 'tipo', $block );

      if( $q = $this->db->get() ){

        okResponse( $block." obtenido", 'data', $q->result_object(), $this );

      }else{

        errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }

      return true;

    });

    $this->response( $result );

  }
    
  public function pbxStatus_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();


      $this->db->select('tipo, json')
              ->select('Last_update')
              ->from('ccexporter.rtMonitor', false)
              ->where_in( 'tipo', $data['block'] );

      if( $q = $this->db->get() ){
          
        $this->db->select("a.asesor,
                            IF(COALESCE(correctPauseType, tipo)=3,'Comida',IF(COALESCE(correctPauseType, tipo)=11,'PNP','Otros')) AS tipoPausa,
                            SUM(IF(COALESCE(b.status,0) != 1,
                                TIME_TO_SEC(Duracion),
                                0))/60 AS Total", FALSE) 
            ->from('asesores_pausas a')
            ->join('asesores_pausas_status b', 'a.id=b.id', 'left')
            ->where( 'inicio >= ', 'CURDATE()', FALSE)
            ->where( 'a.asesor', $_GET['usid'])
//            ->where( 'a.asesor', 31)
            ->group_by(array('a.asesor', 'tipoPausa'));
            
        $pausa = $this->db->get();
        $pausas = array();
          
        foreach( $pausa->result_array() as $index => $info ){
            $pausas[$info['tipoPausa']]=floatVal($info['Total']);
        }
        okResponse( $data['block']." obtenido", 'data', $q->result_object(), $this, 'pausas', $pausas );

      }else{

        errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }

      return true;

    });

    $this->response( $result );

  }

  public function queues_post(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $this->db->select('Skill, Cola, queue, Departamento, monShow, direction, displaySum, sede')
              ->from('Cola_Skill a')
              ->join('PCRCs b', 'a.monShow = b.id', 'left')
              ->where('active', '1')
              ->order_by('Departamento');


      if( $q = $this->db->get() ){

        okResponse( "Colas obtenidas", 'data', $q->result_object(), $this );

      }else{

        errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }

      return true;

    });

    $this->response( $result );

  }
    
  public function pauses_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $this->db->select('*')
              ->from('Tipos_pausas');


      if( $q = $this->db->get() ){
          
          foreach($q->result_array() as $index => $pause){
              $result[$pause['pausa_id']] = $pause;
          }

        okResponse( "Pausas obtenidas", 'data', $result, $this );

      }else{

        errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }

      return true;

    });

    $this->response( $result );

  }

  public function asesorDep_post(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $this->db->select('a.asesor, dep, Departamento')
              ->select('NOMBREASESOR(a.asesor,1) as name', FALSE)
              ->select('IF(cc IS NULL, Departamento, CONCAT(\'PDV \',cc)) as depCC', FALSE)
              ->select('IF(cc IS NULL, color, \'#27b724\') as color', FALSE)
              ->from('dep_asesores a')
              ->join('PCRCs b', 'a.dep = b.id', 'left')
              ->join('cc_apoyo c', 'a.asesor = c.asesor AND CURDATE() BETWEEN inicio AND fin', 'left', FALSE)
              ->where('Fecha = ', 'CURDATE()', FALSE)
              ->where('vacante IS NOT ', 'NULL', FALSE);


      if( $q = $this->db->get() ){

        okResponse( "Deps asesores obtenidos", 'data', $q->result_array(), $this );

      }else{

        errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }

      return true;

    });

    $this->response( $result );

  }

  public function pauseMon_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $date = $this->uri->segment(3);
      segmentSet(  3, "Debe incluir una fecha", $this );
      segmentType( 3, "Debe incluir una fecha en formato YYYY-MM-DD", $this, $type = 'date' );

      $this->db->select('a.*,
                        NOMBREASESOR(a.asesor, 2) AS Nombre,
                        NOMBREDEP(dep) AS Departamento,
                        Pausa,
                        TIME_TO_SEC(Duracion) AS dur_seconds', FALSE)
              ->from('asesores_pausas a')
              ->join('dep_asesores b', 'a.asesor = b.asesor', 'left')
              ->join('Tipos_pausas c', 'a.tipo = c.pausa_id', 'left')
              ->where('Inicio >=', $date);


      if( $q = $this->db->get() ){

        okResponse( "Pausas obtenidas", 'data', $q->result_array(), $this );

      }else{

        errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }

      return true;

    });

    $this->response( $result );

  }


}
