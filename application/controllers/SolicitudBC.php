<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class SolicitudBC extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->helper('mailing');
    $this->load->database();
    $this->load->model('Cliente_model');
  }

  public function baja_solicitud_put(  ){

      $data = $this->put();

      $token = JWT::validateToken( $_GET['token'], $_GET['usn'], 'cAlbertyCome' );

      if( !$token['status'] ){
          $result = array(
                        "status"    => false,
                        "msg"       => $token['msg'],
                        "folio"      => null
                      );
      }else{
        $val = $this->db->query("SELECT COUNT(*) as regs, MAX(fecha_in) as maxFecha FROM asesores_movimiento_vacantes WHERE asesor_in = ".$data['id']." AND fecha_in>='".$data['fechaBaja']."'");
        $validate = $val->row_array();

        if($validate['regs'] > 0){
          errResponse('Existen fechas de registros mayores a la fecha asignada como baja. La baja debe asignarse con una fecha mayor a '.$validate['maxFecha'], REST_Controller::HTTP_BAD_REQUEST, $this, 'error', false);
        }

        $result = $this->bajaSolicitud( $data, $_GET['usid'] );
      }

      jsonPrint( $result );

  }

  public function cambio_solicitud_put(  ){

      $data = $this->put();

      $token = JWT::validateToken( $_GET['token'], $_GET['usn'], 'cAlbertyCome' );

      if( !$token['status'] ){
          $result = array(
                        "status"    => false,
                        "msg"       => $token['msg'],
                        "folio"      => null
                      );
      }else{
        $result = $this->cambioSolicitud( $data );
      }

      jsonPrint( $result );

  }

  public function baja_set_put(  ){

      $data = $this->put();
      $flag = true;

      $token = JWT::validateToken( $_GET['token'], $_GET['usn'], 'cAlbertyCome' );

      if( !$token['status'] ){
          $result = array(
                        "status"    => false,
                        "msg"       => $token['msg'],
                        "folio"      => null
                      );
      }else{

        $val = $this->db->query("SELECT COUNT(*) as regs, MAX(fecha_in) as maxFecha FROM asesores_movimiento_vacantes WHERE asesor_in = ".$data['id']." AND fecha_in>='".$data['fechaBaja']."'");
        $validate = $val->row_array();

        if($validate['regs'] > 0){
          errResponse('Existen fechas de registros mayores a la fecha asignada como baja. La baja debe asignarse con una fecha mayor a '.$validate['maxFecha'], REST_Controller::HTTP_BAD_REQUEST, $this, 'error', false);
        }


        $createSol = $this->bajaSolicitud( $data, $_GET['usid'] );

        if($createSol['status']){
          // okResponse( 'previous setOut Activated', 'data', true, $this );
          $result = $this->bajaSet( $data );
        }else{
          $result = array(
                        "status"    => false,
                        "msg"       => $createSol['msg'],
                        "tabla"     => "Crear Solicitud"
                      );
        }
      }

      if($result['status']){
        $this->db->query("SELECT depAsesores(".$data['id'].",ADDDATE(CURDATE(),365))");
      }

      jsonPrint( $result );

  }

  // DEPRECATED
  public function bajaSet_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      $vac_off = $this->getVacOff($data['id']);

      if($data['approbe']){
        $result = $this->bajaSet( $data );

        if($result['status']){
          $this->db->query("SELECT depAsesores(".$data['id'].",ADDDATE(CURDATE(),180))");
          $result = $this->changeSolicitudStatus($data, 1);

          $q = $this->db->query("SELECT 
                                      NOMBREASESOR(a.asesor, 2) AS Nombre,
                                      NOMBREDEP(dep) AS Departamento,
                                      dep, operacion,
                                      NOMBREASESOR(".$_GET['usid'].", 2) AS sol,
                                      Fecha,
                                      IF(recontratable = 1, 'SI', 'NO') AS Recontratable,
                                      IF(c.status = 1, 'SI', 'NO') AS Reemplazable
                                  FROM
                                      dep_asesores a
                                          LEFT JOIN
                                      asesores_recontratable b ON a.asesor = b.asesor
                                          LEFT JOIN
                                      asesores_plazas c ON a.vacante = c.id
                                  WHERE
                                      a.asesor = ".$data['id']." AND Fecha = '".$data['fechaBaja']."'");
          $mailData = $q->row_array();
          $this->mailing_bajas( $mailData, 1 );

          return $result;
        }else{
          return $result;
        }

      }else{
        $result = $this->changeSolicitudStatus($data, 2);

        return $result;
      }

    });

    jsonPrint( $result );

  }

  public function changeSolicitudStatus( $data, $status ){
    $update = array(
                      'status'         => $status,
                      'aprobado_por'   => $data['applier'],
                      'comentariosRRHH'=> $data['comentariosRRHH']);

    if($this->db->set('fecha_aprobacion = NOW()', false)->set($update)->where('id = '.$data['solicitud'])->update('rrhh_solicitudesCambioBaja')){
      $result = array(
                        'status'    => true,
                        'msg'       => 'Aprobación correctamente cargada. Cambios realizados');
    }else{
      $result = array(
                        'status'    => true,
                        'msg'       => $this->db->error());
    }

    return $result;
  }

  public function cxl_solicitud_delete(){
      
      $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
           
            $id = $this->uri->segment(3);
            $tipo = $this->uri->segment(4);
          
            switch($tipo){
                case '4':
                    $update = array(
                                "status" => 4 ,
                              );
                    $this->db->set('aprobado_por', $_GET['usid']);
                    $this->db->set('fecha_aprobacion', 'NOW()', FALSE);

                    if($this->db->update('rrhh_solicitudesCambioBaja', $update, "id = ".$id)){

                        okResponse( 'Solicitud Cancelada', 'data', true, $this );

                    }else{
                        errResponse('Error al crear contrato', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
                    }
                    break;
                case '5':
                    if($this->db->delete('rrhh_solicitudesCambioBaja', array('id' => $id ))){
                        okResponse( 'Solicitud Borrada', 'data', true, $this );

                    }else{
                        errResponse('Error al crear contrato', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
                    }
                    break;
            }
            
        });

  }

  public function delete_solicitud_delete(){

      $solicitud = $this->uri->segment(3);

      $token = JWT::validateToken( $_GET['token'], $_GET['usn'], 'cAlbertyCome' );

      if( !$token['status'] ){
          $result = array(
                        "status"    => false,
                        "msg"       => $token['msg']
                      );
      }else{
        if($this->db->delete('rrhh_solicitudesCambioBaja', array('id' => $solicitud ))){
          $result = array(
                        "status"    => true,
                        "msg"       => 'Solicitud Eliminada'
                      );
        }else{
          $result = array(
                        "status"    => false,
                        "msg"       => $this->db->error()
                      );
        }

      }

      jsonPrint( $result );

  }

  function getVacOff( $asesor ){
    $query = $this->db->query("SELECT getLastVacante(".$asesor.",1) as Last");
    $result = $query->row();
    $lastMove = $result->Last;

    $query = $this->db->select("vacante, fecha_in")->get_where('asesores_movimiento_vacantes', array( "id" => $lastMove ));
    $result = $query->row();
    $vac_off = $result->vacante;
    $last_fecha_in = $result->fecha_in;

    return array("vac_off" => $vac_off, "last_fecha_in" => $last_fecha_in);
  }

  function setOut( $data, $usr ){
    // okResponse( 'setOut Activated', 'data', true, $this );

    $vo = $this->getVacOff( $data['id'] );

    $vac_off = $vo['vac_off'];
    $last_fecha_in = $vo['last_fecha_in'];

    if( (int)$data['reemplazable'] == 1 ){
      if(date('Y-m-d',strtotime($last_fecha_in))>=date('Y-m-d',strtotime($data['fechaLiberacion']))){

        errResponse("No es posible fijar la ficha final de una vacante que cuenta con cambios posteriores a la fecha de liberacion -> $last_fecha_in || ".$input['fecha_out'], REST_Controller::HTTP_BAD_REQUEST, $this, 'error', 'Error');
        $result = array(
                        'status'  => false,
                        'msg'     => "No es posible fijar la ficha final de una vacante que cuenta con cambios posteriores a la fecha de liberacion -> $last_fecha_in || ".$input['fecha_out']
                      );

        return $result;
      }
    }else{

      $this->notReplace($data, $usr, $vac_off);
    }

    if(date('Y-m-d', strtotime($last_fecha_in))>=date('Y-m-d', strtotime($data['fechaBaja']))){
      errResponse("No es posible asignar cambios con fechas anteriores al ultimo registrado", REST_Controller::HTTP_BAD_REQUEST, $this, 'error', 'Error');
        
      $result = array(
                      'status'  => false,
                      'msg'     => "No es posible asignar cambios con fechas anteriores al ultimo registrado"
                    );

      return $result;
    }

    if( (int)$data['reemplazable'] == 1 ){
      $fout = $data['fechaLiberacion'];
    }else{
      $fout = $data['fechaBaja'];
    }
    $insert = array(
                    'vacante' => $vac_off,
                    'fecha_out' => $fout,
                    'asesor_out' => $data['id']
                  );
    $this->db->set('userupdate', "GETIDASESOR('".str_replace("."," ",$usr)."',2)", FALSE);
    $this->db->set( $insert );

    if($this->db->insert('asesores_movimiento_vacantes', $insert)){
      $result = array(
                      'status'  => true,
                      'msg'     => "Movimiento de salida registrado correctamente",
                      'vac_off' => $vac_off
                    );
    }else{
      errResponse("Error al registrar baja", REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
      $result = array(
                      'status'  => false,
                      'msg'     => $this->db->error()
                    );
    }

    return $result;

  }

  public function setIn($data, $usr){

    $update = array(
                    'fecha_in'    => $data['fecha'],
                    'asesor_in'   => $data['asesor']
                  );
    if($this->db->set('userupdate', "GETIDASESOR('".str_replace("."," ",$usr)."',2)", FALSE)
                ->set($update)
                ->where("id=".$data['movimientoID'])
                ->update("asesores_movimiento_vacantes")){
          $result = array(
                          'status' => true,
                          'msg'    => 'Asesor correctamente ingresado a la vacante');
        }else{
          $result = array(
                          'status' => false,
                          'msg'    => $this->db->error());
        }

    return $result;
  }

  public function notReplace($data, $usr, $vac_off){
    $update = array(
                    'fin' => $data['fechaBaja'],
                    'deactivation_comments' => "Desactivación automática por baja o cambio no reemplazable",
                    'Activo' => 0,
                    'Status' => 2,
                  );
    $this->db->set('deactivated_by', "GETIDASESOR('".str_replace("."," ",$usr)."',2)", FALSE)
              ->set('date_deactivated', "NOW()", FALSE)
              ->set($update)
              ->where("id = ".$vac_off);

    $query = $this->db->update('asesores_plazas');
  }

  // NOT RESTFUL
  public function bajaSolicitud( $data, $usr  ){

        $vo = $this->getVacOff( $data['id'] );

        if($data['tipo'] == 'ask' ){
          $insert = array(
                      "asesor"        => $data['id'],
                      "tipo"          => 2,
                      "fecha"         => $data['fechaBaja'],
                      "reemplazable"  => (int)$data['reemplazable'],
                      "recontratable" => (int)$data['recontratable'],
                      "fecha_replace" => $data['fechaLiberacion'],
                      "comentarios"   => $data['comentarios'],
                      "vac_off"       => $vo['vac_off'],
                      "solicitado_por"=> $usr
                    );
        }else{
          $insert = array(
                      "asesor"        => $data['id'],
                      "tipo"          => 2,
                      "fecha"         => $data['fechaBaja'],
                      "reemplazable"  => (int)$data['reemplazable'],
                      "recontratable" => (int)$data['recontratable'],
                      "fecha_replace" => $data['fechaLiberacion'],
                      "comentariosRRHH" => $data['comentarios'],
                      "vac_off"       => $vo['vac_off'],
                      "comentarios" => "Solicitud creada automáticamente por baja directa en RRHH",
                      "solicitado_por"=> $usr,
                      "status"        => 1
                    );
          $this->db->set('aprobado_por', $usr);
          $this->db->set('fecha_aprobacion', 'NOW()', FALSE);
        }


        $this->db->set('fecha_solicitud', 'NOW()', FALSE);
        $this->db->set( $insert );

        if($this->db->insert('rrhh_solicitudesCambioBaja', $insert)){
          $result = array(
                        "status"    => true,
                        "msg"       => 'Solicitud Guardada Correctamente',
                        "folio"     => $this->db->insert_id()
                      );

                      
                      
          if($data['tipo'] == 'ask'){
            // mailSolicitudBaja::mail( $this, $insert, $vo['vac_off'], 'ask' );
            $this->mailing_bajas( $data, 0 );
          }else{
            $this->mailing_bajas( $data, 0, true );
            $this->mailing_bajas( $data, 1 );
            // mailSolicitudBaja::mail( $this, $insert, $vo['vac_off'], 'ask' );
            // mailSolicitudBaja::mail( $this, $insert, $vo['vac_off'], 'set' );
          }


        }else{
          $result = array(
                        "status"    => false,
                        "msg"       => $this->db->error(),
                        "folio"     => null
                      );
        }



      return $result;

  }

  // NOT RESTFUL
  public function bajaSet( $data ){

      // SET OUT
      $out = $this->setOut( $data, $_GET['usn'] );

      if($out['status']){

          $recontra = onDuplicateUpdate($this, array('asesor'=>$data['id'], 'recontratable' => (int)$data['recontratable']), 'asesores_recontratable');


          $query = $this->db->select('Egreso')->get_where('Asesores', array( "id" => $data['id'] ) );
          $row = $query->row();
          $old_egreso = $row->Egreso;

          $update = array(
                      "Egreso" => $data['fechaBaja']
                    );

          // UPDATE TABLA ASESORES
          if($this->db->update('Asesores', $update, "id = ".$data['id'])){


            // UPDATE HISTORIAL ASESORES
            $insert = array(
                      "asesor"  => $data['id'],
                      "campo"   => 'Egreso',
                      "old_val" => $old_egreso,
                      "new_val" => $data['fechaBaja']
                    );
            $this->db->set('changed_by', "GETIDASESOR('".str_replace("."," ",$_GET['usn'])."',2)", FALSE);
            $this->db->set( $insert );
            if($this->db->insert('historial_asesores', $insert)){
              $result = array(
                            "status"    => true,
                            "msg"       => "Baja registrada en todas las tablas correctamente",
                            "tabla"     => null
                          );

            }else{
              $result = array(
                            "status"    => false,
                            "msg"       => $this->db->error(),
                            "tabla"     => "historial_asesores"
                          );
            }
        }else{
          $result = array(
            "status"    => false,
            "msg"       => $this->db->error(),
            "tabla"     => "Asesores"
          );
        }

      }else{
        $result = array(
                      "status"    => false,
                      "msg"       => $out['msg'],
                      "tabla"     => "movimiento_asesores"
                    );$result = array(
                      "status"    => false,
                      "msg"       => $this->db->error(),
                      "tabla"     => "Asesores"
                    );
      }

      return $result;
  }


  public function cambioSolicitud( $data ){

        $insert = array(
                    "asesor"        => $data['asesor'],
                    "tipo"          => 1,
                    "fecha"         => $data['fechaCambio'],
                    "reemplazable"  => (int)$data['reemplazable'],
                    "fecha_replace" => $data['fechaLiberacion'],
                    "comentarios"   => $data['comentarios'],
                    "status"        => 0,
                    "solicitado_por"=> $data['applier'],
                    "vacante"       => $data['puesto']['vacante'],
                    "movimientoID"  => $data['puesto']['movimientoID']
                  );
        $this->db->set('fecha_solicitud', 'NOW()', FALSE);
        $this->db->set( $insert );

        if($this->db->insert('rrhh_solicitudesCambioBaja', $insert)){
          $vo = $this->getVacOff( $data['asesor'] );

          $result = array(
                        "status"    => true,
                        "msg"       => 'Solicitud Guardada Correctamente',
                        "vo"        => $vo,
                        "folio"     => $this->db->insert_id()
                      );

          mailSolicitudPuesto::mail( $this, $data, $vo['vac_off'], 'ask' );

        }else{
          $result = array(
                        "status"    => false,
                        "msg"       => $this->db->error(),
                        "folio"     => null
                      );
        }

        // $result = array( "status" => true, "query" => $this->db->get_compiled_insert('rrhh_solicitudesCambioBaja', $insert));

      return $result;

  }

  public function addAsesor_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();
      $flag = true;

      // =================================================
      // START Tabla Asesores
      // =================================================
        $asesores = array(
                          'num_colaborador'     => $data['num_colaborador'],
                          'Nombre'              => $data['nombre']." ".$data['apellido'],
                          'Nombre_Separado'     => $data['nombre'],
                          'Apellidos_Separado'   => $data['apellido'],
                          'puesto'              => $data['puesto']['puestoid'],
                          'Activo'              => 1,
                          'on_training'         => 0,
                          'Ingreso'             => $data['fechaCambio'],
                          'Egreso'              => '2030-12-31',
                          'Usuario'             => str_replace(" ",".",strtolower($data['nombre_corto'])),
                          'Esquema'             => $data['puesto']['esquema'],
                          'plaza'               => $data['puesto']['vacante'],
                          'Fecha_Nacimiento'    => $data['Fecha_Nacimiento'] ? $data['Fecha_Nacimiento'] : NULL
                        );
        $this->db->set( '`N Corto`', "'".$data['nombre_corto']."'", FALSE )
                  ->set( '`id Departamento`', "'".$data['puesto']['depid']."'", FALSE );
        if($this->db->set($asesores)->insert('Asesores')){
          $inserted_asesor=$this->db->insert_id();
        }else{
          errResponse('Error al ingresar en Tabla Asesores', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
      // =================================================
      // END Tabla Asesores
      // =================================================

      // =================================================
      // START Tabla userDB
      // =================================================
        $user     = array(
                          'username'            => str_replace(" ",".",strtolower($data['nombre_corto'])),
                          'profile'             => $data['profile'],
                          'asesor_id'           => $inserted_asesor,
                          'active'              => 1,
                          'noAD'                => 0
                        );

        if($this->db->set($user)->insert('userDB')){
          $inserted_userDB=$this->db->insert_id();
        }else{
          $this->deleteAddedAsesor($inserted_asesor, 'Usuarios', $this->db->error());
        }
      // =================================================
      // END Tabla userDB
      // =================================================

      // =================================================
      // START Tabla Supervisores
      // =================================================
        $super    = array(
                          'Fecha'               => $data['fechaCambio'],
                          'asesor'              => $inserted_asesor,
                          'pcrc'                => 0
                        );

        if($this->db->set($super)->insert('Supervisores' )){
          $inserted_super=$this->db->insert_id();
        }else{
          $this->deleteAddedAsesor($inserted_asesor, 'Supervisores', $this->db->error());
        }
      // =================================================
      // END Tabla Supervisores
      // =================================================

      // =================================================
      // START Tabla Contratos
      // =================================================
        $contrato = array(
                        'asesor'              => $inserted_asesor,
                        'tipo'                => $data['tipo_contrato'],
                        'inicio'              => $data['fechaCambio'],
                        'fin'                 => $data['fin_contrato'],
                        'activo'              => 1
                      );
      
        if( $this->db->set($contrato)
            ->set('creator', $_GET['usid'])
            ->set('Last_Update', 'NOW()', FALSE)
            ->insert('asesores_contratos') ){
          $inserted_contrato=$this->db->insert_id();
        }else{
          $this->deleteAddedAsesor($inserted_asesor, 'Contratos', $this->db->error());
        }
      // =================================================
      // END Tabla Contratos
      // =================================================

      // =================================================
      // START Tabla Historial
      // =================================================
        $historial = array(
                          'asesor'              => $inserted_asesor,
                          'campo'               => 'Nuevo Asesor',
                          'old_val'             => '',
                          'new_val'             => '',
                          'changed_by'          => $data['applier']
                        );

        if($this->db->set($historial)->insert('historial_asesores')){
          $inserted_histo=$this->db->insert_id();
        }else{
          $this->deleteAddedAsesor($inserted_asesor, 'Historial', $this->db->error());
        }
      // =================================================
      // END Tabla Historial
      // =================================================

      // =================================================
      // START Tabla Vacantes
      // =================================================
        $move     = array(
                          'fecha_in'            => $data['fechaCambio'],
                          'asesor_in'           => $inserted_asesor,
                          'userupdate'          => $_GET['usid']
                        );

        if( !$this->db->set($move)->where("id", $data['puesto']['movimientoID'])->update('asesores_movimiento_vacantes') ){
          $this->deleteAddedAsesor($inserted_asesor, 'Vacantes', $this->db->error());
        }
      // =================================================
      // END Tabla Vacantes
      // =================================================

      // =================================================
      // START Tabla Salario
      // =================================================
        $salario  = array(
                          'asesor'              => $inserted_asesor,
                          'Fecha'               => $data['fechaCambio'],
                          'factor'              => $data['factor']
                        );

        if( !$this->db->set($salario)->insert('asesores_fcSalario') ){
          $this->deleteAddedAsesor($inserted_asesor, 'Salario', $this->db->error());
        }
      // =================================================
      // END Tabla Salario
      // =================================================

      // =================================================
      // START Tabla DepAsesores
      // =================================================
        if( !$this->db->query("SELECT depAsesores($inserted_asesor, ADDDATE(CURDATE(),365))") ){
          $this->deleteAddedAsesor($inserted_asesor, 'DepAsesores', $this->db->error());
        }
      // =================================================
      // END Tabla DepAsesores
      // =================================================

      okResponse( 'Asesor registrado', 'asesor_id', $inserted_asesor, $this );

    });

    jsonPrint( $result );

  }
  
  private function deleteAddedAsesor( $idAsesor = 0, $table, $error ){

    $this->db->where('id',$idAsesor)->delete('Asesores');
    $this->db->where('asesor_id',$idAsesor)->delete('userDB');
    $this->db->where('asesor',$idAsesor)->delete('Supervisores');
    $this->db->where('asesor',$idAsesor)->delete('asesores_contratos');
    $this->db->where('asesor',$idAsesor)->delete('historial_asesores');
    $this->db->where('asesor_in',$idAsesor)->delete('asesores_movimiento_vacantes');
    $this->db->where('asesor',$idAsesor)->delete('asesores_fcSalario');
    $this->db->where('asesor',$idAsesor)->delete('dep_asesores');

    errResponse('Error al ingresar en Tabla '.$table.'. Registro eliminado', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $error);

  }

  public function solicitudAjuste_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      $this->db->set($data)
              ->set("fecha_solicitud", "NOW()", false )
              ->set(array('status' => 0))
              ->set(array('solicitudActiva' => 1));
      if($this->db->insert('rrhh_solicitudAjusteSalarial')){
        $result = array(
                      'status'  => true,
                      'folio'   => $this->db->insert_id()
                    );
      }else{
        $result = array(
                      'status'  => false,
                      'msg'   => $this->db->error()
                    );
      }

      return $result;

    });

    jsonPrint( $result );

  }

  public function approbeSalario_put(){
    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      $q = $this->db->get_where('rrhh_solicitudAjusteSalarial', 'id = '.$data['id']);
      $solicitud = $q->row_array();

      $q = $this->db->query("SELECT salarioPuesto(".$data['puesto'].", CURDATE()) as salarioPuesto");
      $salario = $q->row_array();

      $factor = floatval($solicitud['nuevo_salario'])/floatval($salario['salarioPuesto']);

      if($data['accept']){
        $update   = array(
                          'status'            => 1,
                          'solicitudActiva'   => $data['id']."-1",
                          'aprobador'         => $data['applier']
                        );
        if($this->db->set($update)
                  ->set("fecha_aprobacion", "NOW()", false )
                  ->where("id = ".$data['id'])
                  ->update('rrhh_solicitudAjusteSalarial')){

                    $fcSalario = array(
                                      'asesor'  => $solicitud['asesor'],
                                      'Fecha'   => $solicitud['fecha_cambio'],
                                      'factor'  => $factor
                                      );
                    if($this->db->set($fcSalario)->insert('asesores_fcSalario')){
                      $result = array('status' => true, 'msg' => "Solicitud Aprobada");

                    }else{
                      $result = array('status' => false, 'msg' => $this->db->error());
                    }



                  }else{
                    $result = array('status' => false, 'msg' => $this->db->error());
                  }

      }else{
        $update   = array(
                          'status'            => 2,
                          'solicitudActiva'   => $data['id']."-4",
                          'aprobador'         => $data['applier']
                        );
        if($this->db->set($update)
                  ->set("fecha_aprobacion", "NOW()", false )
                  ->where("id = ".$data['id'])
                  ->update('rrhh_solicitudAjusteSalarial')){
                    $result = array('status' => true, 'msg' => "Solicitud Declinada");
                  }else{
                    $result = array('status' => false, 'msg' => $this->db->error());
                  }

      }

      return $result;

    });

    jsonPrint( $result );
  }

  public function cxlSalario_put(){
    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      $update   = array(
                        'status'            => 4,
                        'solicitudActiva'   => $data['id']."-4",
                        'aprobador'         => $data['applier']
                      );
      if($this->db->set($update)
                ->set("fecha_aprobacion", "NOW()", false )
                ->where("id = ".$data['id'])
                ->update('rrhh_solicitudAjusteSalarial')){

        $result = array('status' => true, 'msg' => "Solicitud Aprobada");

      }else{
        $result = array('status' => false, 'msg' => $this->db->error());
      }

      return $result;

    });

    jsonPrint( $result );
  }

  public function addContrato_put(){
    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      if($this->db->set($data)->insert('asesores_contratos')){
        $result       = array(
                              'status'  => true,
                              'msg'     => "Contrato Agregado Correctamente"
                            );
      }else{
        $result       = array(
                              'status'  => false,
                              'msg'     => $this->db->error()
                            );
      }

      return $result;

    });

    jsonPrint( $result );
  }

  public function test_get(){

    echo "HOLA";

    $result = array('status' => 'hola como estas');

    echo "Adios";

    return $result;



  }

  public function approbeChange_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();
      $flag = true;

      $q = $this->db->get_where('rrhh_solicitudesCambioBaja', 'id = '.$data['solicitud']);
      $solicitud = $q->row_array();

      $q = $this->getVacOff($solicitud['asesor']);
      $vac_off = $q['vac_off'];

      $mailData   =   array(
                            'asesor'          => $solicitud['asesor'],
                            'fechaCambio'     => $solicitud['fecha'],
                            'reemplazable'    => $solicitud['reemplazable'],
                            'fechaLiberacion' => $solicitud['fecha_replace'],
                            'applier'         => $solicitud['solicitado_por'],
                            'approber'        => $data['applier'],
                            'action'          => $data['accion'],
                            'puesto'          => array('vacante' => $solicitud['vacante'])
                          );

      if($data['accion']){


        $dataOut = array(
                          'id'                => $solicitud['asesor'],
                          'reemplazable'      => $solicitud['reemplazable'],
                          'fechaLiberacion'   => $solicitud['fecha_replace'],
                          'fechaBaja'         => $solicitud['fecha'],
                        );

        $out = $this->setOut($dataOut, $_GET['usn']);

          if($out['status']){

            if($solicitud['reemplazable'] == 0){
              $replace = $this->notReplace($dataOut, $_GET['usn'], $vac_off);
            }

            $in = $this->setIn($solicitud, $_GET['usn']);
              if($in['status']){
                $result = array (
                                  'status'  => true,
                                  'msg'     => "Cambio aplicado correctamente"
                                );
              }else{
                $errors[] = $this->db->error();
                $flag = false;
              }


          }else{
            $errors[] = $out['msg'];
            $flag = false;
          }

        if($flag){

          // DepTable
          $this->db->query("SELECT depAsesores(".$solicitud['asesor'].", ADDDATE(CURDATE(),365))");
          //Update solicitud
          $this->db->set(array('status' => 1))
                    ->set('aprobado_por', "GETIDASESOR('".str_replace("."," ",$_GET['usn'])."',2)", FALSE)
                    ->set('fecha_aprobacion', 'NOW()', FALSE)
                    ->set('comentariosRRHH', $data['comentarios'])
                    ->where('id='.$data['solicitud'])
                    ->update('rrhh_solicitudesCambioBaja');

          // Mail
          mailSolicitudPuesto::mail( $this, $mailData, $vac_off, 'set' );

          return $result;
        }else{
          $result = array('status' => false, 'msg' => $errors);
          return $result;
        }

        return $result;
      }else{

        //Update solicitud
        $this->db->set(array('status' => 3))
                  ->set('aprobado_por', "GETIDASESOR('".str_replace("."," ",$_GET['usn'])."',2)", FALSE)
                  ->set('fecha_aprobacion', 'NOW()', FALSE)
                  ->where('id='.$data['solicitud'])
                  ->update('rrhh_solicitudesCambioBaja');

        mailSolicitudPuesto::mail( $this, $mailData, $vac_off, 'set' );

        return $result = array('status' => true, 'msg' => "Solicitud Declinada");

      }



    });

    jsonPrint( $result );

  }

  public function alreadyOut( $asesor, $fecha ){

    if($query = $this->db->query("SELECT vacante FROM asesores_movimiento_vacantes WHERE asesor_out=$asesor AND fecha_out='$fecha'")){
      $regs = $query->row_array();

      return $regs['vacante'];

    }

  }

  public function pdvChangeOut($asesor, $fecha, $vacanteIn, $vacanteOut){
    // Verifica si existe una salida en la misma fecha
    $lastVacOff = $this->alreadyOut($asesor,$fecha);

    if($lastVacOff != null){

        // Elimina el ultimo registro in con la misma fecha de salida (SETEA A NULL)
        $this->db->query("UPDATE asesores_movimiento_vacantes SET asesor_in = NULL, fecha_in = NULL WHERE asesor_in = $asesor AND fecha_in = '$fecha'");

        // Verifica que la vacante in sea distinta a la que existe como out, si es la misma, borra el registro del out
        if($lastVacOff == $vacanteIn){
          $this->db->query("DELETE FROM asesores_movimiento_vacantes WHERE asesor_out=$asesor AND fecha_out='$fecha'");
          return false;
        }else{
          return true;
        }
    }else{
      $this->db->query("INSERT INTO asesores_movimiento_vacantes (vacante, fecha_out, asesor_out, userupdate) VALUES ($vacanteOut, '$fecha', $asesor, GETIDASESOR('".str_replace("."," ",$_GET['usn'])."',2))");
      return true;
    }
  }

  public function pdvChangeIn($asesor, $fecha, $vacante){
    // Ingresa asesor a nueva plaza
    $this->db->query("UPDATE asesores_movimiento_vacantes SET asesor_in = $asesor, fecha_in='$fecha', userupdate=GETIDASESOR('".str_replace("."," ",$_GET['usn'])."',2) WHERE vacante=$vacante AND asesor_in IS NULL");
  }

  public function chgPDV_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      $f1 = $this->pdvChangeOut($data['asesor_in'], $data['fecha'], $data['vacante_in'], $data['vacante_out']);

      if($data['replaced'] != null){
          if($data['switch']){
            $vOut = $data['vacante_out'];
          }else{
            $vOut = null;
          }
          $f2 = $this->pdvChangeOut($data['replaced'], $data['fecha'], $vOut, $data['vacante_in']);
      }

      if($f1){
        $this->pdvChangeIn($data['asesor_in'], $data['fecha'], $data['vacante_in']);
        $this->db->query("SELECT depAsesores(".$data['asesor_in'].",ADDDATE(CURDATE(),365))");
      }

      if($data['switch']){
        if($f2){
          $this->pdvChangeIn($data['replaced'], $data['fecha'], $data['vacante_out']);
          $this->db->query("SELECT depAsesores(".$data['replaced'].",ADDDATE(CURDATE(),365))");
        }
      }else{
        if($data['replaced'] != null){
            $this->db->query("UPDATE dep_asesores SET vacante = null WHERE asesor=".$data['replaced']." AND Fecha>='".$data['fecha']."'");
            $replaced=", ".$data['replaced'];
        }else{
          $replaced="";
        }
      }




      $res = $this->db->query("SELECT asesor, Fecha, vacante FROM dep_asesores WHERE Fecha='".$data['fecha']."' AND asesor IN (".$data['asesor_in']."$replaced)");

      $validate = $res->result_array();

      $result = array(
                      'data' => $validate
                      );

      return $result;


    });

    $this->response( $result );



  }
    

  public function reIngreso_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();
        $flag = true;

      // =================================================
      // START Tabla Asesores
      // =================================================
        $asesores = array(
                          'Activo'              => 1,
                          'Ingreso'             => $data['fechaCambio'],
                          'Egreso'              => '2030-12-31'
                        );
          
        $this->db->where('id', $data['asesor']);
        if(!$this->db->update( 'Asesores', $asesores )){
            $error['super']=$this->db->error();
            errResponse('Error en la base de datos de Aseosores', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
        }
      // =================================================
      // END Tabla Asesores
      // =================================================

      // =================================================
      // START Tabla Supervisores
      // =================================================
        $super    = array(
                          'Fecha'               => $data['fechaCambio'],
                          'asesor'              => $data['asesor'],
                          'pcrc'                => 0
                        );

        if($this->db->set($super)->insert('Supervisores' )){
          $inserted_super=$this->db->insert_id();
        }else{
          errResponse('Error en la base de datos de Supervisores', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
        }
      // =================================================
      // END Tabla Supervisores
      // =================================================

      // =================================================
      // START Tabla Contratos
      // =================================================
        $contrato = array(
                          'asesor'              => $data['asesor'],
                          'tipo'                => $data['tipo_contrato'],
                          'inicio'              => $data['fechaCambio'],
                          'fin'                 => $data['fin_contrato'],
                        );
        $this->db->set($contrato)
              ->set('creator', $_GET['usid'])
              ->set('Last_Update', 'NOW()', FALSE)
              ->insert('asesores_contratos');
      
        $id = $this->db->insert_id();
        
        if( !$this->db->set(array('activo' => 0, 'updater' => $_GET['usid']))
                            ->set('Last_Update', 'NOW()', FALSE)
                            ->where(array('asesor' => $data['asesor'], 'deleted' => 0, 'id !=' => $id))
                            ->update('asesores_contratos') ){
          errResponse('Error en la base de datos de Contratos', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
        }
      // =================================================
      // END Tabla Contratos
      // =================================================
                              
      // =================================================
      // START Tabla Historial
      // =================================================
        $historial = array(
                          'asesor'              => $data['asesor'],
                          'campo'               => 'Reingreso Asesor',
                          'old_val'             => '',
                          'new_val'             => '',
                          'changed_by'          => $data['applier']
                        );

        if($this->db->set($historial)->insert('historial_asesores')){
          $inserted_histo=$this->db->insert_id();
        }else{
          errResponse('Error en la base de datos de Historial', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
        }
      // =================================================
      // END Tabla Historial
      // =================================================

      // =================================================
      // START Tabla Vacantes
      // =================================================
        $move     = array(
                          'fecha_in'            => $data['fechaCambio'],
                          'asesor_in'           => $data['asesor'],
                          'userupdate'          => $data['applier']
                        );

        if($this->db->set($move)->where("id = ".$data['puesto']['movimientoID'])->update('asesores_movimiento_vacantes')){

        }else{
          errResponse('Error en la base de datos de Movimientos', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
        }
      // =================================================
      // END Tabla Vacantes
      // =================================================

      // =================================================
      // START Tabla Salarios
      // =================================================
        $salario  = array(
                          'asesor'              => $data['asesor'],
                          'Fecha'               => $data['fechaCambio'],
                          'factor'              => $data['factor']
                        );

        if( !$this->db->set($salario)->insert('asesores_fcSalario') ){
          errResponse('Error en la base de datos de Salarios', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
        }
      // =================================================
      // END Tabla Salarios
      // =================================================

      // =================================================
      // START Tabla DepAsesores
      // =================================================
        if( !$this->db->query("SELECT depAsesores(".$data['asesor'].", ADDDATE(CURDATE(),365))") ){
          errResponse('Error en la base de datos de DepAsesores', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
        }
      // =================================================
      // END Tabla DepAsesores
      // =================================================

      okResponse( 'Reingreso correcto', 'data', true, $this );

    });

    jsonPrint( $result );

  }
    
    function contrato_chgActive_get(){
    
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
           
            $asesor = $this->uri->segment(3);
            $id = $this->uri->segment(4);
            
            $this->db->set(array('activo' => 0, 'updater' => $_GET['usid']))
                ->set('Last_Update', 'NOW()', FALSE)
                ->where(array('asesor' => $asesor, 'deleted' => 0));
            
            if( $this->db->update('asesores_contratos') ){
                
                $this->db->set(array('activo' => 1, 'updater' => $_GET['usid']))
                    ->set('Last_Update', 'NOW()', FALSE)
                    ->where(array('id' => $id));
                if( $this->db->update('asesores_contratos') ){
                    okResponse( 'Contrato activado', 'data', true, $this );
                }else{
                    errResponse('Error al activar contrato', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
                }
                
            }else{
                errResponse('Error al desactivar contratos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }
            
        });

    }
    
    function contrato_delete_get(){
    
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
           
            $id = $this->uri->segment(3);
            
            $this->db->set(array('deleted' => 1, 'deleter' => $_GET['usid']))
                ->set('date_deleted', 'NOW()', FALSE)
                ->where(array('id' => $id));
            
            if( $this->db->update('asesores_contratos') ){
                
                okResponse( 'Contrato Eliminado', 'data', true, $this );
                
            }else{
                errResponse('Error al eliminar contrato', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }
            
        });

    }
    
    function contrato_add_put(){
    
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
           
            $data = $this->put();
            
            $this->db->set($data)
                ->set('creator', $_GET['usid'])
                ->set('Last_Update', 'NOW()', FALSE);
            
            
            if( $this->db->insert('asesores_contratos') ){
                
                $id = $this->db->insert_id();
                
                if( $data['activo'] == 1 ){
                    $this->db->set(array('activo' => 0, 'updater' => $_GET['usid']))
                            ->set('Last_Update', 'NOW()', FALSE)
                            ->where(array('asesor' => $data['asesor'], 'deleted' => 0, 'id !=' => $id))
                            ->update('asesores_contratos');
                }
                
                okResponse( 'Contrato Agregado', 'data', true, $this );
                
            }else{
                errResponse('Error al crear contrato', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }
            
        });

    }

    public function testMail_get(){

      $asesor = $this->uri->segment(3);
      $fecha = $this->uri->segment(4);
      $reemp = $this->uri->segment(5);
      $recont = $this->uri->segment(6);

      $reemp = $reemp == 1 ? true : false;
      $recont = $recont == 1 ? true : false;
        
      require_once(APPPATH.'controllers/Mailing.php'); //include controller
      $mail = new Mailing();  //create object 
      $mail->bajaSolicitud( $asesor, $_GET['usid'], $fecha, $reemp, $recont ); //call function
    }

    public function addAsesorV2_put(){

      $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
  
        $data = $this->put();
        $asesor = $data['fields'];
        $asesor['Usuario'] = str_replace(' ', '.', strtolower($asesor['N Corto']));
        $asesor['Nombre'] = $asesor['Nombre_Separado']." ".$asesor['Apellidos_Separado'];
        $asesor['Egreso'] = '20301231';
        unset($asesor['Pais']);
        unset($asesor['vacante']);
        unset($asesor['N Corto']);
        unset($asesor['profile']);
        unset($asesor['contrato']);
        unset($asesor['fin_contrato']);
  
        $flag = true;
  
        // =================================================
        // START Tabla Asesores
        // =================================================
          $this->db->set( '`N Corto`', "'".$data['fields']['N Corto']."'", FALSE );
          if($this->db->set($asesor)->insert('Asesores')){
            $inserted_asesor=$this->db->insert_id();
          }else{
            errResponse('Error al ingresar en Tabla Asesores', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
          }
        // =================================================
        // END Tabla Asesores
        // =================================================
  
        // =================================================
        // START Tabla userDB
        // =================================================
          $user     = array(
                            'username'            => $asesor['Usuario'],
                            'profile'             => $data['fields']['profile'],
                            'asesor_id'           => $inserted_asesor,
                            'active'              => 1,
                            'noAD'                => 0
                          );
  
          if($this->db->set($user)->insert('userDB')){
            $inserted_userDB=$this->db->insert_id();
          }else{
            $this->deleteAddedAsesor($inserted_asesor, 'Usuarios', $this->db->error());
          }
        // =================================================
        // END Tabla userDB
        // =================================================
  
        // =================================================
        // START Tabla Supervisores
        // =================================================
          $super    = array(
                            'Fecha'               => $asesor['Ingreso'],
                            'asesor'              => $inserted_asesor,
                            'pcrc'                => 0
                          );
  
          if($this->db->set($super)->insert('Supervisores' )){
            $inserted_super=$this->db->insert_id();
          }else{
            $this->deleteAddedAsesor($inserted_asesor, 'Supervisores', $this->db->error());
          }
        // =================================================
        // END Tabla Supervisores
        // =================================================
  
        // =================================================
        // START Tabla Contratos
        // =================================================
          $contrato = array(
                          'asesor'              => $inserted_asesor,
                          'tipo'                => $data['fields']['contrato'],
                          'inicio'              => $asesor['Ingreso'],
                          'fin'                 => $data['fields']['fin_contrato'],
                          'activo'              => 1
                        );
        
          if( $this->db->set($contrato)
              ->set('creator', $_GET['usid'])
              ->set('Last_Update', 'NOW()', FALSE)
              ->insert('asesores_contratos') ){
            $inserted_contrato=$this->db->insert_id();
          }else{
            $this->deleteAddedAsesor($inserted_asesor, 'Contratos', $this->db->error());
          }
        // =================================================
        // END Tabla Contratos
        // =================================================
  
        // =================================================
        // START Tabla Historial
        // =================================================
          $historial = array(
                            'asesor'              => $inserted_asesor,
                            'campo'               => 'Nuevo Asesor',
                            'old_val'             => '',
                            'new_val'             => '',
                            'changed_by'          => $_GET['usid']
                          );
  
          if($this->db->set($historial)->insert('historial_asesores')){
            $inserted_histo=$this->db->insert_id();
          }else{
            $this->deleteAddedAsesor($inserted_asesor, 'Historial', $this->db->error());
          }
        // =================================================
        // END Tabla Historial
        // =================================================
  
        // =================================================
        // START Tabla Vacantes
        // =================================================
          $move     = array(
                            'fecha_in'            => $asesor['Ingreso'],
                            'asesor_in'           => $inserted_asesor,
                            'userupdate'          => $_GET['usid']
                          );
  
          if( !$this->db->set($move)->where("id", $data['fields']['vacante'])->update('asesores_movimiento_vacantes') ){
            $this->deleteAddedAsesor($inserted_asesor, 'Vacantes', $this->db->error());
          }
        // =================================================
        // END Tabla Vacantes
        // =================================================
  
        // =================================================
        // START Tabla Salario (INACTIVO)
        // =================================================
          // $salario  = array(
          //                   'asesor'              => $inserted_asesor,
          //                   'Fecha'               => $data['fechaCambio'],
          //                   'factor'              => $data['factor']
          //                 );
  
          // if( !$this->db->set($salario)->insert('asesores_fcSalario') ){
          //   $this->deleteAddedAsesor($inserted_asesor, 'Salario', $this->db->error());
          // }
        // =================================================
        // END Tabla Salario
        // =================================================
  
        // =================================================
        // START Tabla DepAsesores
        // =================================================
          if( !$this->db->query("SELECT depAsesores($inserted_asesor, ADDDATE(CURDATE(),365))") ){
            $this->deleteAddedAsesor($inserted_asesor, 'DepAsesores', $this->db->error());
          }
        // =================================================
        // END Tabla DepAsesores
        // =================================================
  
        okResponse( 'Asesor registrado', 'asesor_id', $inserted_asesor, $this );
  
      });
  
      jsonPrint( $result );
  
    }

    public function reIngresoV2_put(){

      $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
  
        $data = $this->put();
        $asesor = $data['fields'];
        $asesor['Usuario'] = str_replace(' ', '.', strtolower($asesor['N Corto']));
        $asesor['Nombre'] = $asesor['Nombre_Separado']." ".$asesor['Apellidos_Separado'];
        $asesor['Egreso'] = '20301231';
        unset($asesor['Pais']);
        unset($asesor['vacante']);
        unset($asesor['N Corto']);
        unset($asesor['profile']);
        unset($asesor['contrato']);
        unset($asesor['fin_contrato']);
        
        $flag = true;
  
        // =================================================
        // START Tabla Asesores
        // =================================================
          
          $oldId = $data['asesorId'];
       
          $this->db->where('id', $oldId)
                  ->set('`N Corto`', "'".$data['fields']['N Corto']."'", FALSE);
          if(!$this->db->update( 'Asesores', $asesor )){
              errResponse('Error en la base de datos de Asesores', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
          }
        // =================================================
        // END Tabla Asesores
        // =================================================

        // =================================================
        // START Tabla Usuarios
        // =================================================

          $params = array(
            'username' => $asesor['Usuario'],
            'active'  => 1,
            'profile' => $data['fields']['profile'],
            'noAD' => 0
          );
      
          $this->db->where('asesor_id', $oldId);
          if(!$this->db->update( 'Asesores', $params )){
              errResponse('Error en la base de datos de Usuarios', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
          }
        // =================================================
        // END Tabla Usuarios
        // =================================================
  
        // =================================================
        // START Tabla Supervisores
        // =================================================
          $super    = array(
                            'Fecha'               => $asesor['Ingreso'],
                            'asesor'              => $oldId,
                            'pcrc'                => 0
                          );
  
          if($this->db->set($super)->insert('Supervisores' )){
            $inserted_super=$this->db->insert_id();
          }else{
            errResponse('Error en la base de datos de Supervisores', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
          }
        // =================================================
        // END Tabla Supervisores
        // =================================================
  
        // =================================================
        // START Tabla Contratos
        // =================================================
          $contrato = array(
                            'asesor'              => $oldId,
                            'tipo'                => $data['contrato'],
                            'inicio'              => $asesor['Ingreso'],
                            'fin'                 => $fin['fin_contrato'],
                          );
          $this->db->set($contrato)
                ->set('creator', $_GET['usid'])
                ->set('Last_Update', 'NOW()', FALSE)
                ->insert('asesores_contratos');
        
          $id = $this->db->insert_id();
          
          if( !$this->db->set(array('activo' => 0, 'updater' => $_GET['usid']))
                              ->set('Last_Update', 'NOW()', FALSE)
                              ->where(array('asesor' => $oldId, 'deleted' => 0))
                              ->update('asesores_contratos') ){
            errResponse('Error en la base de datos de Contratos', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
          }
        // =================================================
        // END Tabla Contratos
        // =================================================
                                
        // =================================================
        // START Tabla Historial
        // =================================================
          $historial = array(
                            'asesor'              => $oldId,
                            'campo'               => 'Reingreso Asesor',
                            'old_val'             => '',
                            'new_val'             => '',
                            'changed_by'          => $_GET['usid']
                          );
  
          if($this->db->set($historial)->insert('historial_asesores')){
            $inserted_histo=$this->db->insert_id();
          }else{
            errResponse('Error en la base de datos de Historial', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
          }
        // =================================================
        // END Tabla Historial
        // =================================================
  
        // =================================================
        // START Tabla Vacantes
        // =================================================
          $move     = array(
                            'fecha_in'            => $asesor['Ingreso'],
                            'asesor_in'           => $oldId,
                            'userupdate'          => $_GET['usid']
                          );
  
          if($this->db->set($move)->where("id = ".$data['fields']['vacante'])->update('asesores_movimiento_vacantes')){
  
          }else{
            errResponse('Error en la base de datos de Movimientos', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
          }
        // =================================================
        // END Tabla Vacantes
        // =================================================
  
        // =================================================
        // START Tabla Salarios INACTIVO
        // =================================================
          // $salario  = array(
          //                   'asesor'              => $data['asesor'],
          //                   'Fecha'               => $data['fechaCambio'],
          //                   'factor'              => $data['factor']
          //                 );
  
          // if( !$this->db->set($salario)->insert('asesores_fcSalario') ){
          //   errResponse('Error en la base de datos de Salarios', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
          // }
        // =================================================
        // END Tabla Salarios
        // =================================================
  
        // =================================================
        // START Tabla DepAsesores
        // =================================================
          if( !$this->db->query("SELECT depAsesores(".$oldId.", ADDDATE(CURDATE(),365))") ){
            errResponse('Error en la base de datos de DepAsesores', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
          }
        // =================================================
        // END Tabla DepAsesores
        // =================================================
  
        okResponse( 'Reingreso correcto', 'data', true, $this );
  
      });
  
      jsonPrint( $result );
  
    }

    // MAILING FUNCTIONS
    private function countReturn($q, $msg, $err = false){
      if( $q->num_rows() == 0 ){
          if( $err ){
              // errResponse($msg, REST_Controller::HTTP_BAD_REQUEST, $this, 'error', false);
          }else{
              // okResponse($msg, 'data', true, $this);
          }
      }
  }
  
  private function sendMail( $titulo, $user, $tipo, $body ){
      $msg = mailingV2::msg_encapsule($titulo, $body);
          
      if( mailingV2::send($user, $titulo, $msg) ){
          $this->db->query("INSERT INTO mail_dailyCheck VALUES (NULL, '$tipo', CURDATE(), '$user', 1, NULL) ON DUPLICATE KEY UPDATE sent = 1");    
      }else{
          $this->db->query("INSERT INTO mail_dailyCheck VALUES (NULL, '$tipo', CURDATE(), '$user', 0, NULL) ON DUPLICATE KEY UPDATE sent = 0");
      }
  }
  
  private function getMailList( $tipo ){
      $mailQ = $this->db->query("SELECT 
                                      a.*, NOMBREASESOR(asesor_id, 1) AS Nombre, sent
                                  FROM
                                      mail_lists a
                                          LEFT JOIN
                                      userDB b ON a.usuario = b.username
                                          LEFT JOIN
                                      mail_dailyCheck c ON a.usuario = c.user
                                          AND c.Fecha = CURDATE()
                                          AND a.notif = c.tipo
                                  WHERE
                                      notif = '$tipo'
                                          AND COALESCE(sent, 0) = 0");
      
      $this->countReturn($mailQ, 'Sin mails pendientes');
      return $mailQ->result_array();
      
  }
  
  private function getMailListNV( $tipo ){
      $mailQ = $this->db->query("SELECT 
                                      a.*, NOMBREASESOR(asesor_id, 1) AS Nombre
                                  FROM
                                      mail_lists a
                                          LEFT JOIN
                                      userDB b ON a.usuario = b.username
                                  WHERE
                                      notif = '$tipo'");
      
      $this->countReturn($mailQ, 'Sin mails configurados para tipo \'$tipo\'');
      return $mailQ->result_array();
  }
  
  public function mailing_bajas( $mailData, $tipo, $auto = false ){

      $q = $this->db->query("SELECT 
                              NOMBREASESOR(a.asesor, 2) AS Nombre,
                              NOMBREDEP(dep) AS Departamento,
                              dep, operacion,
                              NOMBREASESOR(".$_GET['usid'].", 2) AS sol,
                              Fecha,
                              IF(recontratable = 1, 'SI', 'NO') AS Recontratable,
                              IF(c.status = 1, 'SI', 'NO') AS Reemplazable
                          FROM
                              dep_asesores a
                                  LEFT JOIN
                              asesores_recontratable b ON a.asesor = b.asesor
                                  LEFT JOIN
                              asesores_plazas c ON a.vacante = c.id
                          WHERE
                              a.asesor = ".$mailData['id']." AND Fecha = '".$mailData['fechaLiberacion']."'");
      $data = $q->row_array();
    
      if( $tipo == 1 ){
        $list = "bajaOK_";
        $titulo = "Baja Procesada para ".$data['Nombre'];
        $tipoSol = "Autorizado por";
        $greet = "procesado correctamente";
      }else{
        $list = "bajaSOL_";
        $titulo = "Baja solicitada para ".$data['Nombre'];
        $tipoSol = "Solicitado por";
        $greet = "solicitado";
      }

      if( $auto ){
        $sol = "Generada automáticamente por baja directa en RRHH";
      }else{
        $sol = $data['sol'];
      }

      $mails = $this->getMailList( $list.$data['operacion'] );
      
      $body = "<div><table style='text-align: left'>\n
      <tr><th style='padding: 5px; border: 1px solid #d5d3d3;'>Nombre</th>
      <th style='padding: 5px; border: 1px solid #d5d3d3;'>Departamento</th>
      <th style='padding: 5px; border: 1px solid #d5d3d3;'>$tipoSol</th>
      <th style='padding: 5px; border: 1px solid #d5d3d3;'>Fecha Baja</th>
      <th style='padding: 5px; border: 1px solid #d5d3d3;'>Comentarios</th>
      <th style='padding: 5px; border: 1px solid #d5d3d3;'>Reemplazable</th>
      <th style='padding: 5px; border: 1px solid #d5d3d3;'>Recontratable</th></tr>\n";
      
      $body .= "<tr><td style='padding: 5px; border: 1px solid #d5d3d3;'>".$data['Nombre']."</td>
                    <td style='padding: 5px; border: 1px solid #d5d3d3;'>".$data['Departamento']."</td>
                    <td style='padding: 5px; border: 1px solid #d5d3d3;'>".$sol."</td>
                    <td style='text-align: center; padding: 5px; border: 1px solid #d5d3d3;'>".$data['Fecha']."</td>
                    <td style='text-align: center; padding: 5px; border: 1px solid #d5d3d3;'>".$mailData['comentarios']."</td>
                    <td style='text-align: center; padding: 5px; border: 1px solid #d5d3d3;'>".$data['Reemplazable']."</td>
                    <td style='text-align: center; padding: 5px; border: 1px solid #d5d3d3;'>".$data['Recontratable']."</td></tr>\n";

      
      $body .= "</table></div><br>\n";

      $flag = false;
      
      foreach( $mails as $index => $info ){
          if( $info['usuario'] == $_GET['usn'] ){
            $flag = true;
          }
          $text = '';
          $text = "<p>Hola ".$info['Nombre'].",</p><p>La siguiente baja se ha $greet:</p>".$body;
          $this->sendMail($titulo, $info['usuario'], 'bajaOK - '.$data['Nombre'], $text);
      }

      if( !$flag ){
        $text = '';
        $text = "<p>¡Hola!</p><p>La siguiente baja se ha $greet:</p>".$body;
        $this->sendMail($titulo,  $_GET['usn'], 'bajaOK - '.$data['Nombre'], $text);
      }
      
      return true;
  }  


}
