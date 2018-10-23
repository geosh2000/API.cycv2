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

  public function avisosPdv_get(){
    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        $this->db->select("*,
                          NOMBREASESOR(asesor, 1) AS Creador,
                          NOMBREASESOR(updater, 1) AS Modifico", FALSE)
            ->from('avisos_pdv')
            ->where('dtCreated >=', 'CURDATE()', FALSE);
        
        if( $q = $this->db->get() ){
            
            okResponse( 'Info Obtenida', 'data', $q->result_array(), $this, 'filters', null);
            
        }else{
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

        return true;
    });
  
    jsonPrint( $result );
  }

  public function avisosPdvUpd_Put(){
    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        $data = $this->put();

        $st = 0;
        if( $data['status'] ){
          $st = 1;
        }

        $upd = array(
          'status' => $st,
          'updater' => $_GET['usid']
        );

        $this->db->set($upd)->where('id', $data['id']);
        
        if( $q = $this->db->update('avisos_pdv') ){
            
            okResponse( 'Info Actualizada', 'data', true, $this, 'filters', null);
            
        }else{
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

        return true;
    });
  
    jsonPrint( $result );
  }

  public function pdvAdvSave_put(){
    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        $data = $this->put();

        $this->db->set($data)->set('asesor', $_GET['usid']);
        
        if( $q = $this->db->insert('avisos_pdv') ){
            
            okResponse( 'Info Guardada', 'data', true, $this, 'filters', null);
            
        }else{
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

        return true;
    });
  
    jsonPrint( $result );
  }

  public function deletePdvAdv_put(){
    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        $data = $this->put();

        $this->db->where($data);
        
        if( $q = $this->db->delete('avisos_pdv') ){
            
            okResponse( 'Info Borrada', 'data', true, $this, 'filters', null);
            
        }else{
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

        return true;
    });
  
    jsonPrint( $result );
  }

  public function avisosPdvReport_get(){
    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        $inicio = $this->uri->segment(3);
        $fin = $this->uri->segment(4);

        $this->db->select("id,CAST(dtCreated as DATE) AS Fecha,
                            CAST(dtCreated as TIME) AS Hora,
                            NOMBREPDV(pdv, 3) AS PDV,
                            NOMBREPDV(pdv, 4) AS Ciudad,
                            localizador,
                            NOMBREASESOR(asesor, 2) AS Asesor,
                            aviso,
                            IF(status = 1, 'Revisado', '') AS Estado,
                            NOMBREASESOR(updater, 2) AS ModificadoPor,
                            Last_Update AS UltimaModificacion", FALSE)
                  ->from('avisos_pdv')
                  ->where('dtCreated >=', $inicio)
                  ->where('dtCreated <', "ADDDATE('$fin',1)",FALSE)
                  ->order_by('dtCreated');
        
        if( $q = $this->db->get() ){
            
            okResponse( 'Info Obtenida', 'data', $q->result_array(), $this);
            
        }else{
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

        return true;
    });
  
    jsonPrint( $result );
  }
}

