<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Smd extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->helper('mailing');
    $this->load->database();
    $this->load->model('Cliente_model');
  }

    public function updateUsers_put(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $data = $this->put();
            $err = array();
            $suc = array();

            foreach($data['usr'] as $i => $r){
                $u = $r;
                $a = $r['id'];
                unset($u['id']);

                $q = $this->db->set($u)
                        ->set('id', "$a", FALSE)
                        ->set('cycId', "GETBYUSER('".$u['smdId']."')", FALSE)
                        ->get_compiled_insert('smd_users');
                $q .= " ON DUPLICATE KEY UPDATE Nombre=VALUES(Nombre), Apellido=VALUES(Apellido),job=VALUES(job), role=VALUES(role)";

                if( !$this->db->query( $q ) ){
                    array_push($err, array('id' => $r['id'], 'err' => $this->db->error()) );
                }else{
                    array_push($suc, $r['id']);
                }
            }

            if( count($err) == 0 ){
                okResponse( 'Guardado', true, count($suc), $this, 'regs', count($suc) );
            }else{
                errResponse('Error al insertar '.count($err).' registros', REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $err);
            }

        });


    }

    public function upload_put(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $data = $this->put();
            $err = array();
            $suc = array();
            $attr = array();

            foreach($data['main'] as $i => $r){
                $u = $r;
                $a = $r['asesor'];
                unset($u['asesor']);

                $q = $this->db->set($u)->set('asesor', "SMDUSER($a)", FALSE)->get_compiled_insert('smd_tx_main');
                $q .= " ON DUPLICATE KEY UPDATE dtUpdate=VALUES(dtUpdate), asesor=VALUES(asesor)";

                if( !$this->db->query( $q ) ){
                    array_push($err, array('id' => $r['id'], 'err' => $this->db->error()) );
                }else{
                    array_push($suc, $r['id']);
                }
            }

            foreach($data['attr'] as $i => $ra){

                if( isset($ra['val']) ){

                    $q = $this->db->set($ra)->get_compiled_insert('smd_tx_attr');
                    $q .= " ON DUPLICATE KEY UPDATE val=VALUES(val)";

                    if( !$this->db->query( $q ) ){
                        array_push($err, array('id' => $ra['txMainId'], 'attr' => $ra['atributo'], 'err' => $this->db->error()) );
                    }else{
                        array_push($attr, $ra['txMainId']);
                    }
                }else{
                    $this->db->where('txMainId',$ra['txMainId'])->where('atributo',$ra['atributo'])->delete('smd_tx_attr');
                }
            }

            if( count($err) == 0 ){
                okResponse( 'Guardado', true, count($suc), $this, 'regs', count($suc) );
            }else{
                errResponse('Error al insertar '.count($err).' registros', REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $err);
            }



        });

    }

}