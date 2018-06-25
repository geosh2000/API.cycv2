<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Config extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->database();
  }

  public function addExternal_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      $data = $this->put();
      $flag = true;

      //DB Asesores
      $asesores = array(
                        'Nombre'              => $data['nombre']." ".$data['apellido'],
                        'Nombre_Separado'     => $data['nombre'],
                        'Apellidos_Separado'  => $data['apellido'],
                        'Egreso'              => '2030-12-31',
                        'Usuario'             => str_replace(" ",".",strtolower($data['nombre_corto'])),
                        'Esquema'             => 8,
                        'plaza'               => ""
                      );
      $this->db->set( '`N Corto`', "'".$data['nombre_corto']."'", FALSE )
                ->set( '`Ingreso`', "CURDATE()", FALSE )
                ->set( '`id Departamento`', "47", FALSE );
      if($this->db->set($asesores)->insert('Asesores')){
          $inserted_asesor=$this->db->insert_id();
      }else{
        $flag = false;
        $error['Asesores']=$this->db->error();
      }


      // userDB
      if($flag){
        $user     = array(
                          'username'            => str_replace(" ",".",strtolower($data['nombre_corto'])),
                          'profile'             => $data['profile'],
                          'asesor_id'           => $inserted_asesor,
                          'active'              => 1,
                          'noAD'                => $data['validation']
                        );
        if($data['validation'] == 1){
          $this->db->set('hashed_pswd', "$2y$10$2He4.0svP7aCUsrLjjZxLuxOJ1dh1hRPF5IzXIWvnfLH603HH2yMC");
        }

        if($this->db->set($user)->insert('userDB')){
          $inserted_userDB=$this->db->insert_id();
          okResponse( 'Usuario cargado correctamente con id: '.$inserted_userDB, 'data', true, $this );
        }else{
          errResponse('Error en la base de datos', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
        }
      }


      return $result;

    });

    $this->response($result);

  }

  public function getReportsUpdate_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        $this->db->select("*", FALSE)
                ->from("config_reportUpdates")
                ->order_by('name');

        if( $q = $this->db->get() ){
          okResponse( 'Reportes obtenidos: ', 'data', $q->result_array(), $this );
        }else{
          errResponse('Error en la base de datos', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
        }

      return true;

    });

    $this->response($result);
  }
    
    public function locCreator_put(){
        
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
            
            $locs = $this->put();

            $this->db->select("Localizador, Afiliado, IF(asesor = -1, 'Online',IF(asesor = 0,'Unknown',NOMBREASESOR(asesor,2))) as Creador", FALSE)
                    ->from("t_Locs")
                    ->group_by("Localizador")
                    ->where_in('Localizador',$locs)
                    ->order_by('Localizador');

            if( $q = $this->db->get() ){
              okResponse( 'Locs obtenidos: ', 'data', $q->result_array(), $this );
            }else{
              errResponse('Error en la base de datos', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
            }

          return true;

        });

        $this->response($result);
        
    }
    
    
    public function locCreatorChange_put(){
        
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
            
            $data = $this->put();

            $q = $this->db->set("asesor", $data['asesor'])
                    ->set("Nombre ","NOMBREASESOR(".$data['asesor'].",1)", FALSE)
                    ->where_in('Localizador',$data['locs']);

            if( $q = $this->db->update('t_Locs') ){
                
                $this->db->set("asesor", $data['asesor'])
                    ->set("Nombre ","NOMBREASESOR(".$data['asesor'].",1)", FALSE)
                    ->where_in('Localizador',$data['locs']);

                if( $q = $this->db->update('d_Locs') ){
                  okResponse( 'Locs actualizados: ', 'data', true, $this );
                }else{
                  errResponse('Error en la base d_Locs (t_locs actualizado)', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
                }

            }else{
              errResponse('Error en la base t_Locs', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
            }

          return true;

        });

        $this->response($result);
        
    }

}
