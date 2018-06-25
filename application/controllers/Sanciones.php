<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Sanciones extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('jwt');
    $this->load->database();
  }

  public function sanciones_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $asesor = $this->uri->segment(3);
      $inicio = $this->uri->segment(4);
      $fin = $this->uri->segment(5);

      $this->db->select("*, nombreAsesor(asesor,2) as nombre_asesor, nombreAsesor(registered_by,1) as creador, IF(CURDATE() BETWEEN fecha_afectacion_inicio AND fecha_afectacion_fin,1,0) as afectado");

      if($asesor > 0){
        $this->db->where(array('asesor' => $asesor));
      }

      if(isset($inicio)){
        $this->db->where(array('fecha_aplicacion >=' => $inicio, 'fecha_aplicacion <=' => $fin));
      }

      $this->db->order_by('fecha_aplicacion', 'DESC');

      if($query = $this->db->get('Sanciones')){

        foreach($query->result() as $row){
            $data[]=$row;
        }

        $result = array(
                      "status"    => true,
                      "msg"       => "Información obtenida",
                      "rows"      => $query->num_rows()
                    );

        if($query->num_rows()>0){
          $result['data'] = $data;
        }else{
          $result['data'] = null;
        }

      }else{
        $result = array(
                      "status"    => false,
                      "msg"       => $this->db->error(),
                      "rows"      => 0,
                      "data"      => null
                    );
      }

      return $result;

    });

    jsonPrint($result);

  }


}
