<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Preferences extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->database();
  }

    public function userPreferences_get(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $this->db->select('*')->from('userDB')->where('asesor_id', $_GET['usid']);
                
            if( $pQ = $this->db->get() ){  
                $preferences = $pQ->row_array(); 
                okResponse( 'Preferencias Obtenidas', 'data', $preferences, $this );
            }else{
                errResponse('Error al obtener listado', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

        });

    }

    public function zonasHorarias_get(){

        $this->db->select('*')->from('config_zonasHorarias')->where('id', $this->uri->segment(3));
            
        if( $zQ = $this->db->get() ){  
            $zH = $zQ->row_array(); 
            okResponse( 'Zona Horaria Obtenida', 'data', $zH, $this );
        }else{
            errResponse('Error al obtener listado', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

    }

    public function listZonasHorarias_get(){

        $this->db->select('*')->from('config_zonasHorarias');
            
        if( $zQ = $this->db->get() ){  
            $zH = $zQ->result_array(); 
            okResponse( 'Zona Horaria Obtenida', 'data', $zH, $this );
        }else{
            errResponse('Error al obtener listado', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

    }

    public function setPref_put(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $params = $this->put();

            $this->db->set($params)->where('asesor_id', $_GET['usid']);
                
            if( $this->db->update('userDB') ){  
                okResponse( 'Preferencia guardada', 'data', true, $this );
            }else{
                errResponse('Error al obtener listado', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

        });

    }

}