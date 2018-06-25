<?php
defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Clientes extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->database();
    $this->load->model('Asesores_model');
  }

public function cliente_get(){

    $asesor_id = $this->uri->segment(3);

    // Validacion
    if( !isset($asesor_id) ){

      $respuesta = array(
                        'ERR' => true,
                        'msg' => "Definir id del cliente" );

      $this->response( $respuesta, REST_Controller::HTTP_BAD_REQUEST );
      return;
    }

    $cliente = $this->Asesores_model->get_asesor( $asesor_id );

    if( isset($cliente) ){

      $respuesta = array(
                        'ERR'     => FALSE,
                        'msg'     => "Registro cargado correctamente",
                        'cliente' => $cliente );

      $this->response( $respuesta );

    }else{

      $respuesta = array(
                        'ERR'     => TRUE,
                        'msg'     => "El cliente con el id $asesor_id no existe",
                        'cliente' => null );

      $this->response( $respuesta, REST_Controller::HTTP_NOT_FOUND );

    }

  }

}
