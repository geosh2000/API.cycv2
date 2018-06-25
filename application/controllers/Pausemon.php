<?php
defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');

class Pausemon extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->database();
  }

  public function pauseMon_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $date = $this->uri->segment(3);
      segmentSet(  3, "Debe incluir una fecha", $this );
      segmentType( 3, "Debe incluir una fecha en formato YYYY-MM-DD", $this, $type = 'date' );
        
      $asesor = $this->uri->segment(4);
        

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS deletePauses");
      $this->db->query("CREATE TEMPORARY TABLE deletePauses SELECT * FROM asesores_pausas WHERE CAST(Inicio as DATE)>=ADDDATE(CURDATE(),-1)");
      $this->db->query("SELECT PAUSECALC(asesor, Inicio, Fin) as tmp FROM deletePauses");

      $this->db->select('a.id, a.asesor, IF(correctPauseType IS NULL, a.tipo, correctPauseType) as tipo, Inicio, Fin, Duracion, Skill, a.Last_Update, correctPauseType, changed_by, change_date,
                        NOMBREASESOR(a.asesor, 2) AS Nombre,
                        NOMBREDEP(dep) AS Departamento,
                        Pausa, Productiva,
                        TIME_TO_SEC(Duracion) AS dur_seconds,
                        NOMBREASESOR(reg_by,1) as reg_by,
                        caso, status, notas', FALSE)
              ->from('asesores_pausas a')
              ->join('dep_asesores b', 'a.asesor = b.asesor AND CAST(Inicio AS DATE) = b.Fecha', 'left', FALSE)
              ->join('Tipos_pausas c', 'IF(correctPauseType IS NULL, a.tipo, correctPauseType) = c.pausa_id', 'left', FALSE)
              ->join('asesores_pausas_status d', 'a.id = d.id', 'left')
              ->where('a.asesor !=', 0)
              ->where('Inicio >=', $date)
              ->where('Inicio <=', $date." 23:59:59")
              ->order_by('Inicio');
        
      if( isset($asesor) ){
          $this->db->where('a.asesor', $asesor);
      }

      // okResponse( "Pausas obtenidas", 'data', $this->db->get_compiled_select(), $this );

      if( $q = $this->db->get() ){

        $luQ = $this->db->query("SELECT MAX(Last_Update) as Last_Update FROM asesores_pausas WHERE CAST(Inicio as DATE)>=CURDATE()");
        $lu = $luQ->row_array();

        okResponse( "Pausas obtenidas", 'data', array( 'data' => $q->result_array(), 'lu' => $lu['Last_Update']), $this );

      }else{

        errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());

      }

      return true;

    });

    $this->response( $result );

  }

  public function justify_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();
      // okResponse( "Pausas obtenidas", 'data', $data, $this );

      $insert = array(
                        'asesor'  => $data['asesor'],
                        'Fecha'   => $data['fecha'],
                        'id'      => $data['pausa'],
                        'status'  => $data['status'],
                        'caso'    => $data['caso'],
                        'notas'   => $data['notas']
     );

      $this->db->set($insert)
              ->set('reg_by ', "GETIDASESOR('".$_GET['usn']."',3)", FALSE);

      $query = $this->db->get_compiled_insert('asesores_pausas_status')." ON DUPLICATE KEY UPDATE status = ".$data['status'].", caso = '".$data['caso']."', notas='".$data['notas']."'";

      if( $this->db->query($query) ){

        okResponse( "Status guardado", 'data', true, $this );

      }else{

        errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error(), 'query', $query);

      }

      return true;

    });

    $this->response( $result );

  }

  public function delete_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();
      // okResponse( "Pausas obtenidas", 'data', $data, $this );

      $id = $data['pausa'];

      if( $this->db->query("DELETE FROM asesores_pausas_status WHERE id=$id") ){

        okResponse( "Status guardado", 'data', true, $this );

      }else{

        errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error(), 'query', $query);

      }

      return true;

    });

    $this->response( $result );

  }
    
    public function pauseTypes_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){


      if( $q = $this->db->query("SELECT * FROM Tipos_pausas WHERE pausa_id>0 ORDER BY Pausa") ){

        okResponse( "Status guardado", 'data', $q->result_array(), $this );

      }else{

        errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error(), 'query', $query);

      }

      return true;

    });

    $this->response( $result );

  }
    
    public function pauseChange_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        $data = $this->put();
        
        $this->db->set('correctPauseType', $data['tipo'])
                ->set('changed_by', $data['changed_by'])
                ->set('change_date ', "NOW()", FALSE)
                ->where('id',$data['id']);


      if( $this->db->update("asesores_pausas") ){

        okResponse( "Status guardado", 'data', TRUE, $this );

      }else{

        errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error(), 'query', $query);

      }

      return true;

    });

    $this->response( $result );

  }


}
