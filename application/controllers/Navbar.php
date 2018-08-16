<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Navbar extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('jwt');
    $this->load->helper('validators');
    $this->load->database();
  }

  public function getMenu_get(){

    if($q = $this->db->query("SELECT * FROM menu WHERE activo=1 ORDER BY parent, titulo")){

      $menu = $q->result_array();

      foreach($menu as $index => $data){
        $navbar[$data['level']][$data['parent']][] = array(
                                                    'title'         => str_replace("<br>", " ", $data['titulo']),
                                                    'href'          => $data['liga'],
                                                    'credential'    => $data['permiso'],
                                                    'id'            => $data['id'],
                                                    'v2link'        => $data['v2link'],
                                                    'v2Active'      => $data['v2Active']
                                                  );

      }

      $this->response($navbar);
    }else{
      errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
    }
  }

  public function menuRAW_get(){
    if($q = $this->db->query("SELECT * FROM menu WHERE activo=1 ORDER BY parent, titulo")){
      okResponse('Menu cargado', 'data', $q->result_array(), $this);
    }else{
      errResponse('Error al cargar menú', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
    }
  }
}
