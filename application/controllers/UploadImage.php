<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class UploadImage extends REST_Controller {

  public function __construct(){

    parent::__construct();
    
    $this->output->set_header("Acess-Control-Allow-Origin:*");

    $this->load->helper('json_utilities');
    $this->load->helper('jwt');
    $this->load->helper('validators');
    $this->load->database();

  }

  public function uploadImage_post(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->post();

      if(!isset($data['ftype'])){
        $data['ftype']=".jpg";
      }

      $result = $this->saveImage( $data['fname'], $data['dir'], $data['ftype'] );

      // $result = $_FILES;

      return $result;

    });

    $this->response($result);


  }

  public function saveImage( $fname, $dir, $ftype = ".jpg" ){

    	$target_dir   = $_SERVER['DOCUMENT_ROOT']."/img/".$dir."/";
      $upload_dir   = $_SERVER['DOCUMENT_ROOT']."/img/".$dir;
    	$target_file  = $target_dir . $fname;
    	$uploadOK     = 1;
    	$FileType     = $ftype;
    	$filename     = $target_file . $FileType;

      if (!is_dir($upload_dir)){
        mkdir($upload_dir, 0777, true);
      }

      if(is_writable($upload_dir)) {
        if (move_uploaded_file($_FILES["image"]["tmp_name"], $filename)) {

            return array(
                          "ERR"   => false,
                          "msg"   => "Imagen subida correctamente");

      	}else{

            return array(
                          "ERR"   => true,
                          "msg"   => "No se pudo subir la imagen. Error desconocido");
        }

      }else{

        return array(
                      "ERR"   => true,
                      "msg"   => 'El directorio no tiene permiso de escritura');
      }


  }

  public function fileExists_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $dir = $this->uri->segment(3);
      $fname = $this->uri->segment(4);
      $ext = $this->uri->segment(5);

      $url = $_SERVER['DOCUMENT_ROOT']."/img/".$dir."/".$fname.".".$ext;

      if( file_exists( $url ) ){
        $result = array(
                        'ERR' => false,
                        'msg' => 'Fichero existe',
                        'url' => $url);
      }else{
        $result = array(
                        'ERR' => true,
                        'msg' => 'Fichero NO existe',
                        'url' => $url);
      }

      return $result;

    });

    $this->response($result);

  }

  public function imageDel_delete(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $dir = $this->uri->segment(3);
      $fname = $this->uri->segment(4);
      $ext = $this->uri->segment(5);

      $url = $_SERVER['DOCUMENT_ROOT']."/img/".$dir."/".$fname.".".$ext;

      if( file_exists( $url ) ){
        if(unlink( $url )){
          $result = array(
                          'ERR' => false,
                          'msg' => 'Fichero borrado',
                          'url' => $url);
        }else{
          $result = array(
                          'ERR' => true,
                          'msg' => 'Hubo un error al intentar borrar el fichero. El fichero sigue activo',
                          'url' => $url);
        }

      }else{
        $result = array(
                        'ERR' => true,
                        'msg' => 'Fichero NO existe. No se puede borrar.',
                        'url' => $url);
      }

      return $result;

    });

    $this->response($result);

  }

}
