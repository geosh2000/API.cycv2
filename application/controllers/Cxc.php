<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Cxc extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('jwt');
    $this->load->helper('validators');
    $this->load->database();
  }

  public function addcxc_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      $data['firmado']=(int)$data['firmado'];
      if($this->db->insert('asesores_cxc', $data)){
        $result = array(
                      "status"    => true,
                      "msg"       => "Cxc guardado correctamente",
                      "folio"      => $this->db->insert_id()
                    );
      }else{
        $result = array(
                      "status"    => false,
                      "msg"       => $this->db->error(),
                      "folio"      => null
                    );
      }

      return $result;

    });

    jsonPrint( $result );

  }

  public function obtener_saldos_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $asesor = $this->uri->segment(3);

      $this->db->select("a.id, pago as quincena, a.monto, asesor, localizador")
              ->from('rrhh_pagoCxC as a')
              ->join('asesores_cxc as b', 'a.cxc=b.id', 'LEFT')
              ->join('rrhh_calendarioNomina as c', 'a.quincena = c.id', 'LEFT')
              ->where(array('activo' => 1, 'cobrado' => 0, 'asesor' => $asesor))
              ->order_by('pago');

      if($query = $this->db->get()){

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

  public function saldar_cxc_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      foreach($data as $item => $flag){

        if( $item == 'saldado_por' ){
          $applier = $flag;
        }else{
          if($flag == 'true'){
            $where[] = $item;
          }
        }

      }

      if(isset($where)){
        if($this->db->where_in('id', $where)->update('rrhh_pagoCxC', array( 'cobrado' => 1, "saldado_por" => $applier ))){
          $result = array(
                        "status"    => true,
                        "msg"       => "Items Saldados Correctamente",
                        "saldados"  => $where
                      );
        }else{
          $result = array(
                        "status"    => false,
                        "msg"       => $this->db->error()
                      );
        }
      }else{
        $result = array(
                      "status"    => true,
                      "msg"       => "No existen items seleccionados para saldar"
                    );
      }

      return $result;

    });

    jsonPrint( $result );

  }

  function getToApply_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $asesor = $this->uri->segment(3);
      $filter = $this->uri->segment(4);

      if($asesor == 0){
        $where = array('a.status' => 1 );
        switch ($filter) {
          case 'pdv':
            $where['dep'] = 29;
            break;
          case 'cc':
            $where['dep !='] = 29;
            break;
        }
      }else{
        $where = array('a.asesor' => $asesor, 'status !=' => 2);
      }

      $this->db->select('a.*, nombreAsesor(a.created_by, 1) as creador, nombreAsesor(a.asesor, 2) as nombreAsesor')
                ->from('asesores_cxc a')
                ->join('dep_asesores b', 'a.asesor=b.asesor AND CURDATE()=b.Fecha', 'LEFT')
                ->where($where);
      if($query = $this->db->get()){

        foreach($query->result() as $row){
          $data[] = $row;
        }

        $result = array(
                        'status'  => true,
                        'msg'     => 'Información obtenida correctamente',
                        'rows'    => $query->num_rows()
                        );

        if($query->num_rows()>0){
          $result['data']=$data;
        }else{
          $result['data']=null;
        }

      }else{
        $result = array(
                        'status'  => false,
                        'msg'     => $this->db->error()
                        );
      }

      return $result;

    });

    $this->response( $result );

  }

  public function getCalendario_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $limit = $this->uri->segment(3);

      if( !isset($limit) ){
        $limit = 5;
      }

      $this->db->where('ADDDATE(CURDATE(),-'.(16*$limit).') <', 'fin', FALSE)
                ->order_by('inicio')
                ->limit($limit);
      if($query = $this->db->get('rrhh_calendarioNomina')){

        foreach($query->result() as $row){
          $data[] = $row;
        }

        $result = array(
                        'status'  => true,
                        'msg'     => 'Información obtenida correctamente',
                        'rows'    => $query->num_rows()
                        );

        if($query->num_rows()>0){
          $result['data']=$data;
        }else{
          $result['data']=null;
        }

      }else{
        $result = array(
                        'status'  => false,
                        'msg'     => $this->db->error()
                        );
      }

      return $result;

    });

    jsonPrint( $result );

  }

  public function applyCxc_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();
      $quincena = $data['inicio'];

      for($i=1; $i<=$data['quincenas']; $i++){

        $inserts[] = array(
                        'cxc'        => $data['id'],
                        'n_pago'    => $i,
                        'quincena'  => $quincena,
                        'monto'     => $data['monto']/$data['quincenas'],
                        'activo'    => 1,
                        'cobrado'   => 0,
                        'created_by'=> $data['created_by']
                      );
        $quincena++;
      }

      if($query = $this->db->insert_batch('rrhh_pagoCxC',$inserts)){

        $this->db->update('asesores_cxc', array('status' => 2), "id = ".$data['id']);

        $result = array(
                        'status'  => true,
                        'msg'     => 'Cxc aplicado correctamente'
                        );

      }else{
        $result = array(
                        'status'  => false,
                        'msg'     => $this->db->error()
                        );
      }

      return $result;

    });

    jsonPrint( $result );


  }

  public function getAllCxc_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $inicio = $this->uri->segment(3);
      $fin    = $this->uri->segment(4);

      if($query = $this->db->select("a.*, NOMBREASESOR(asesor,2) as NombreAsesor, NOMBREASESOR(created_by,1) as NombreCreador, NOMBREASESOR(updated_by,1) as NombreAplicador")
                        ->select("CASE WHEN status = 0 THEN 'Pendiente de Envío' WHEN status = 1 THEN 'Esperando RRHH' WHEN status = 2 THEN 'Aplicado' END as statusOK")
                        ->select("CASE WHEN tipo = 0 THEN 'Responsabilidad' WHEN tipo = 1 THEN 'Colaborador' END as tipoOK")
                        ->get_where('asesores_cxc a',"fecha_aplicacion BETWEEN '$inicio' AND '$fin'")){

        $arreglo = $query->result_array();

        foreach($arreglo as $index => $item){

          $url = $_SERVER['DOCUMENT_ROOT']."/img/cxc/".$item['id'].".jpg";

          if( file_exists( $url ) ){
            $arreglo[$index]['fileExist'] = 1;
          }else{
            $arreglo[$index]['fileExist'] = 0;
          }

        }

        $result = array(
                        'status'  => true,
                        'rows'    => $query->num_rows(),
                        'data'    => $arreglo
                        );

      }else{
        $result = array(
                        'status'  => false,
                        'msg'     => $this->db->error()
                        );
      }

      return $result;

    });

    $this->response( $result );

  }

  public function edit_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      $update = array(
                      'comments'    => $data['comments'],
                      'firmado'     => (int)$data['firmado'],
                      'monto'       => $data['monto'],
                      'updated_by'  => $data['applier']
                    );

      if($this->db->set($update)->where(array('id' => $data['id'] ))->update('asesores_cxc')){
        $result = array(
                          'status'  => true,
                          'msg'     => "Registro Actualizado"
                          );
      }else{
        $result = array(
                          'status'  => true,
                          'msg'     => $this->db->error()
                          );
      }

      return $result;

    });

    jsonPrint( $result );

  }

  public function statusChange_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      $update = array('status' => 1, 'updated_by' => $data['applier'] );

      if($this->db->set($update)->where(array('id' => $data['id'] ))->update('asesores_cxc')){
        $result = array(
                          'status'  => true,
                          'msg'     => "Registro Actualizado"
                          );
      }else{
        $result = array(
                          'status'  => true,
                          'msg'     => $this->db->error()
                          );
      }

      return $result;

    });

    jsonPrint( $result );

  }

  public function aplicadosCorte_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $quincena = $this->uri->segment(3);

      segmentSet( 3, 'Se debe indicar un id de quincena', $this);
      segmentType( 3, 'El id de quincena debe ser numérico', $this);

      $this->db->query("SET @quincena = $quincena");

      // Tabla temporal con info de cxc
      $this->db->select("cxc,
                        COUNT(*) as pagos,
                        SUM(IF(cobrado = 1, monto, 0)) as montoPagado", FALSE)
                ->from("rrhh_pagoCxC")
                ->group_by("cxc");

      $tmpTable = $this->db->get_compiled_select();
      $this->db->query("DROP TEMPORARY TABLE IF EXISTS cxcSummary");
      $this->db->query("CREATE TEMPORARY TABLE cxcSummary $tmpTable");

    // Tabla temporal con info de historicos
      $this->db->select("cxc,
                        COUNT(*) as moves", FALSE)
                ->from("cxc_historic")
                ->group_by("cxc");
        $tmpHistoric = $this->db->get_compiled_select();
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS cxcHistoric");
        $this->db->query("CREATE TEMPORARY TABLE cxcHistoric $tmpHistoric");

      // Query para data
      $this->db->select("NULL as sel,
                        a.id,
                        a.cxc as pago_ID,
                      	NOMBREASESOR(b.asesor,5) as num_Colaborador,
                      	NOMBREASESOR(b.asesor,4) as asesor,
                        b.localizador,
                        n_pago,
                        d.pagos as n_pagos,
                        a.monto as monto_Quincena,
                    	  montoPagado as monto_Pagado,
                        b.monto as monto_Total,
                    	  b.monto - montoPagado as monto_Saldo,
                        cobrado as pago_Status,
                        b.tipo,
                        b.status as cxc_Status, e.moves as historic", FALSE)
                ->from("rrhh_pagoCxC a")
                ->join("asesores_cxc b"           ,"a.cxc = b.id"       ,"LEFT")
                ->join("rrhh_calendarioNomina c"  ,"a.quincena = c.id"  ,"LEFT")
                ->join("cxcSummary d"             ,"a.cxc = d.cxc"      ,"LEFT")
                ->join("cxcHistoric e"           ,"a.id = e.cxc"      ,"LEFT")
                ->where( array( 'c.id' => $quincena ));

      if($data = $this->db->get()){
          if($data->num_rows()>0){
              okResponse( 'Información obtenida', 'data', $data->result_array(), $this );
          }else{
              errResponse('No existen registros', REST_Controller::HTTP_PARTIAL_CONTENT, $this, 'data', $data->result_array());
          }
      }else{
          errResponse('Error en la base de datos', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
      }

    });

    $this->response($result);

  }

  public function chgStatus_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

        $ids = implode(',',  $data['ids']);
        $comment = "Registro generado automáticamente por el sistema";

        $query = $this->db->query("INSERT INTO cxc_historic (SELECT
                          NULL, id , NULL, GETIDASESOR('".$_GET['usn']."',3), 'status cobro', cobrado, ".$data['status'].", '$comment' FROM
                          rrhh_pagoCxC WHERE id IN ($ids) AND cobrado!=".$data['status'].")");

        $array = array(
                'cobrado'   => $data['status']
        );

        $this->db->where_in('id', $data['ids']);

      if($this->db->update('rrhh_pagoCxC', $array)){
            okResponse( 'Status modificado', 'data', true, $this );

      }else{
            errResponse('Error en la base de datos', REST_Controller::HTTP_NOT_IMPLEMENTED, $this, 'error', $this->db->error());
      }

    });

    $this->response($result);

  }

    public function getHistoric_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $id = $this->uri->segment(3);

      if($query = $this->db->query("SELECT id, cxc as pago_ID, fecha, NOMBREASESOR(user,1) as Usuario, cambio, old_value as valor_anterior, new_value as valor_actual, comments FROM cxc_historic WHERE cxc=$id")){
            okResponse( 'Data obtenida', 'data', $query->result_array(), $this );

      }else{
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
      }

    });

    $this->response($result);

  }

  public function editAmmount_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      if($data['saldo']<0){
        $query = $this->db->query("SELECT id, monto, monto+(".$data['saldo'].") as flag FROM rrhh_pagoCxC WHERE cxc=".$data['cxc']." ORDER BY quincena DESC LIMIT 1");
        $flagRes = $query->row();
        if($flagRes->flag < 0){
          errResponse('No es posible asignar un monto que supere el asignado a la última quincena. El monto asignado al pago id: '.$flagRes->id.' es $'.$flagRes->monto, REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
      }

      $query = $this->db->query("INSERT INTO cxc_historic (SELECT
                        NULL, id , NULL, GETIDASESOR('".$_GET['usn']."',3), 'monto', monto, ".$data['newAmmount'].", '".$data['comments']."' FROM
                        rrhh_pagoCxC WHERE id=".$data['id'].")");

      $array = array(
                'monto' => $data['newAmmount']
      );

      $this->db->where('id', $data['id'])
                ->update('rrhh_pagoCxC', $array);

      if($data['saldo']>0){

        $q = $this->db->query("SELECT MAX(n_pago)+1 as pago, MAX(quincena)+1 as q, GETIDASESOR('".$_GET['usn']."',3) as creador FROM rrhh_pagoCxC WHERE cxc=".$data['cxc']);
        $dataQ = $q->row();

        $insert = array(
                  'cxc'     => $data['cxc'],
                  'n_pago'  => $dataQ->pago,
                  'quincena'=> $dataQ->q,
                  'monto'   => $data['saldo'],
                  'activo'  => 1,
                  'cobrado' => 0,
                  'historic'=> "Creado por modificación de pago id: ".$data['id'],
                  'created_by' => $dataQ->creador
        );

        if($this->db->insert('rrhh_pagoCxC', $insert)){

          $newId = $this->db->insert_id();

          $query = $this->db->query("INSERT INTO cxc_historic (SELECT
                            NULL, id , NULL, GETIDASESOR('".$_GET['usn']."',3), 'nueva quincena', null, ".$data['saldo'].", historic FROM
                            rrhh_pagoCxC WHERE id=$newId)");

              okResponse( 'Saldo actualizado. Se ha agregado un pago adicional quedando ahora '.$dataQ->pago.' pagos', 'data', true, $this );

        }else{
              errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
      }else{
        $q = $this->db->query("SELECT id, n_pago as pago, quincena as q, monto, GETIDASESOR('".$_GET['usn']."',3) as creador FROM rrhh_pagoCxC WHERE cxc=".$data['cxc']." ORDER BY quincena DESC LIMIT 1");
        $dataQ = $q->row();

        $query = $this->db->query("INSERT INTO cxc_historic (SELECT
                          NULL, id , NULL, GETIDASESOR('".$_GET['usn']."',3), 'monto', monto, monto+(".$data['saldo']."), 'monto modificado por cambio de monto en pago: ".$data['id']."' FROM
                          rrhh_pagoCxC WHERE id=".$dataQ->id.")");

        $update = array('monto' => ($dataQ->monto + $data['saldo']));
        $this->db->where(array('id' => $dataQ->id));
        if($this->db->update('rrhh_pagoCxC', $update)){

              okResponse( 'Saldo actualizado. El último pago ('.$dataQ->pago.' de '.$dataQ->pago.') ha sido modificado también', 'data', true, $this );

        }else{
              errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

      }

    });

    $this->response($result);

  }

  // ==================================================
  // Version 2.0
  // ==================================================

  public function transactions_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $localizador = $this->uri->segment(3);
      $rev = $this->uri->segment(4);

      if( $rev != 1 ){
        $this->db->select('a.id, a.localizador, NOMBREASESOR(asesor,1) as asesor, SUM(montoFiscal) as Monto, cxcIdLink, a.status')
          ->from('asesores_cxc a')
          ->join('cxc_transactions b', 'a.id=cxcIdLink', 'left')
          ->where(array('a.localizador' => $localizador))
          ->group_by(array('a.id'))
          ->order_by('fecha_cxc');
      }else{
        $this->db->select('a.id, a.Localizador, aplicablePara, montoFiscal, cxcIdLink, COALESCE(status,0) as status')
          ->from('cxc_transactions a')
          ->join('asesores_cxc b', 'b.id=cxcIdLink', 'left')
          ->where(array('a.localizador' => $localizador))
          ->order_by('dtCreated');
      }

      
      if($q = $this->db->get()){

            okResponse( 'Transacciones obtenidas', 'data', $q->result_array(), $this );

      }else{
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
      }

    });

    $this->response($result);

  }

  public function cxcAsesor_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $asesor = $this->uri->segment(3);

      $this->db
          ->select("a.cxcId")
          ->select("Localizador")
          ->select("montoFiscal AS montoTotal")
          ->select("maxParcialidad AS montoQuincenal")
          ->select("SUM(IF(IF(a.status = 1
                      AND CURDATE() >= ADDDATE(payday, - 2),
                  2,
                  IF(a.status = 0
                          AND CURDATE() >= ADDDATE(payday, - 2),
                      - 1,
                      a.status)) = 2,
              montoParcial,
              0)) AS montoPagado",FALSE)
          ->select("SUM(IF(IF(a.status = 1
                      AND CURDATE() >= ADDDATE(payday, - 2),
                  2,
                  IF(a.status = 0
                          AND CURDATE() >= ADDDATE(payday, - 2),
                      - 1,
                      a.status)) != 2,
              montoParcial,
              0)) AS montoPendiente",FALSE)
          ->select("CONCAT(COALESCE(COUNT(IF(IF(a.status = 1
                                      AND CURDATE() >= ADDDATE(payday, - 2),
                                  2,
                                  IF(a.status = 0
                                          AND CURDATE() >= ADDDATE(payday, - 2),
                                      - 1,
                                      a.status)) = 2,
                              a.id,
                              NULL)),
                          0),
                  ' de ',
                  COALESCE(COUNT(a.id), 0)) AS parcialidades",FALSE)
          ->from('cxc_payTable a')
          ->join('asesores_cxc b','a.cxcId = b.id','left')
          ->not_group_start()
            ->where('a.status',1)
            ->where('montoParcial',0)
          ->group_end()
          ->where('b.asesor',$asesor)
          ->group_by('Localizador')
          ->having('montoPendiente >', 0)
          ->order_by('Localizador');
      
      $query = $this->db->get_compiled_select();
      // okResponse("Pagos Obtenidos", "data", $query, $this);

      if($q = $this->db->query($query)){
        okResponse("Pagos Obtenidos", "data", $q->result_array(), $this);
      }else{
        errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
      }

      return $result;

    });

    $this->response( $result );

  }

  public function downloadCxcAdmin_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $payday = $this->uri->segment(3);

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS sumary");
      $this->db->query("CREATE TEMPORARY TABLE sumary SELECT 
          cxcId,
          montoFiscal,
          maxParcialidad,
          SUM(IF(IF(status = 1 AND CURDATE() >= ADDDATE(payday,-2),2,IF(status=0 AND CURDATE() >= ADDDATE(payday,-2),-1,status)) = 2, montoParcial, 0)) AS paidMonto,
          SUM(IF(IF(status = 1 AND CURDATE() >= ADDDATE(payday,-2),2,IF(status=0 AND CURDATE() >= ADDDATE(payday,-2),-1,status)) != 2, montoParcial, 0)) AS pendienteMonto,
          COUNT(*) AS parcialidades,
          COUNT(IF(IF(status = 1 AND CURDATE() >= ADDDATE(payday,-2),2,IF(status=0 AND CURDATE() >= ADDDATE(payday,-2),-1,status)) = 2, id, NULL)) AS paidParc
      FROM
          cxc_payTable
      WHERE 
          NOT (status=1 AND montoParcial = 0)
      GROUP BY cxcId");
      $this->db->query("ALTER TABLE sumary ADD PRIMARY KEY (cxcId)");

      $querySCXC = "SELECT 
                        NOMBREASESOR(b.asesor, 5) AS clave,
                        IF(b.tipo = 0, 'SCXC', 'RESERVA') AS clave_saldo,
                        'cd' AS compania_fiscal_defecto,
                        ROUND(pendienteMonto, 2) AS saldo_inicial,
                        ROUND(montoParcial, 2) AS monto_descuento,
                        'true' AS calculo_normal,
                        'true' AS calculo_finiquito,
                        Localizador AS referencia,
                        'normal' AS tipo_nomina
                    FROM
                        cxc_payTable a
                            LEFT JOIN
                        asesores_cxc b ON a.cxcId = b.id
                            LEFT JOIN
                        sumary c ON a.cxcId = c.cxcId
                    WHERE
                        montoParcial > 0 AND payday = '$payday'
                            AND b.tipo = 0";
        
      $queryRESERVA ="SELECT 
                        NOMBREASESOR(b.asesor, 5) AS clave,
                        IF(b.tipo = 0, 'SCXC', 'RESERVA') AS clave_saldo,
                        'cd' AS compania_fiscal_defecto,
                        ROUND(pendienteMonto, 2) AS saldo_inicial,
                        ROUND(montoParcial, 2) AS monto_descuento,
                        'true' AS calculo_normal,
                        'true' AS calculo_finiquito,
                        Localizador AS referencia,
                        'normal' AS tipo_nomina
                    FROM
                        cxc_payTable a
                            LEFT JOIN
                        asesores_cxc b ON a.cxcId = b.id
                            LEFT JOIN
                        sumary c ON a.cxcId = c.cxcId
                    WHERE
                        montoParcial > 0 AND payday = '$payday'
                            AND b.tipo = 1";
        
      $querySALDOS = "SELECT 
                        NOMBREASESOR(b.asesor, 5) AS clave,
                        UPPER(NOMBREASESOR(b.asesor, 4)) AS Empleado,
                        IF(Egreso > payday,
                            'active',
                            'inactive') AS Estatus_Empleado,
                        IF(b.tipo = 0,
                            'Descuento CXC',
                            'Descuento Reservas Empleado') AS Concepto,
                        date_created AS Fecha_de_captura,
                        ROUND(c.montoFiscal, 2) AS saldo_inicial,
                        ROUND(pendienteMonto, 2) AS saldo_actual,
                        ROUND(montoParcial, 2) AS desc_pendiente,
                        'true' AS calculo_normal,
                        'true' AS calculo_finiquito,
                        Localizador AS referencia,
                        'normal' AS tipo_nomina
                    FROM
                        cxc_payTable a
                            LEFT JOIN
                        asesores_cxc b ON a.cxcId = b.id
                            LEFT JOIN
                        sumary c ON a.cxcId = c.cxcId
                            LEFT JOIN
                        Asesores d ON b.asesor = d.id
                    WHERE
                        montoParcial > 0 AND payday = '$payday'";

      $result = array();

      $scxc = $this->db->query($querySCXC);
      $reservas = $this->db->query($queryRESERVA);
      $saldos = $this->db->query($querySALDOS);

      $result['scxc'] = $scxc->result_array();
      $result['reservas'] = $reservas->result_array();
      $result['saldos'] = $saldos->result_array();

      okResponse("Pagos Obtenidos", "data", $result, $this);


      return $result;

    });

    $this->response( $result );

  }

  public function cxcCorte_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $payday = $this->uri->segment(3);
      $byId = $this->uri->segment(4);

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS sumary");
      $this->db->query("CREATE TEMPORARY TABLE sumary SELECT 
          cxcId,
          montoFiscal,
          maxParcialidad,
          SUM(IF(IF(status = 1 AND CURDATE() >= ADDDATE(payday,-2),2,IF(status=0 AND CURDATE() >= ADDDATE(payday,-2),-1,status)) = 2, montoParcial, 0)) AS paidMonto,
          SUM(IF(IF(status = 1 AND CURDATE() >= ADDDATE(payday,-2),2,IF(status=0 AND CURDATE() >= ADDDATE(payday,-2),-1,status)) != 2, montoParcial, 0)) AS pendienteMonto,
          COUNT(*) AS parcialidades,
          COUNT(IF(IF(status = 1 AND CURDATE() >= ADDDATE(payday,-2),2,IF(status=0 AND CURDATE() >= ADDDATE(payday,-2),-1,status)) = 2, id, NULL)) AS paidParc
      FROM
          cxc_payTable
      WHERE 
          NOT (status=1 AND montoParcial = 0)
      GROUP BY cxcId");
      $this->db->query("ALTER TABLE sumary ADD PRIMARY KEY (cxcId)");

      $this->db->select('a.id')
                ->select('a.cxcId')
                ->select('a.payday')
                ->select('NOMBREASESOR(c.asesor,5) as Colaborador', FALSE)
                ->select('NOMBREASESOR(c.asesor,2) as Asesor', FALSE) 
                ->select('Localizador')
                ->select('IF(a.status = 1 AND CURDATE() >= ADDDATE(payday,-2),2,IF(a.status=0 AND CURDATE() >= ADDDATE(payday,-2),-1,a.status)) as status', FALSE)
                ->select('montoParcial as montoQuincena')
                ->from('cxc_payTable a')
                ->join('sumary b', 'a.cxcId=b.cxcId', 'left')
                ->join('asesores_cxc c', 'a.cxcId = c.id', 'left')
                ->not_group_start()
                ->where('montoParcial',0)
                ->where('a.status',1)
                ->group_end();

      if( isset($byId) && $byId == 1 ){
        $this->db->select('a.montoFiscal as montoTotalCxc')
                ->select('consecutivo as numeroParcialidad')
                ->select('parcialidades as totalParcialidades')
                ->where('a.cxcId', $payday);
      }else{
        $this->db->select('a.maxParcialidad as montoParcialidades')
                ->select('paidMonto as  montoPagado')
                ->select('pendienteMonto as montoPendiente')
                ->select('a.montoFiscal as montoTotalCxc')
                ->select('paidParc as parcialidadesPagadas')
                ->select('consecutivo as numeroParcialidad')
                ->select('parcialidades as totalParcialidades')
                ->where('payday', $payday);
      }

      $query = $this->db->get_compiled_select();

      if($q = $this->db->query($query)){
        okResponse("Pagos Obtenidos", "data", $q->result_array(), $this);
      }else{
        errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
      }

      return $result;

    });

    $this->response( $result );

  }

  public function editPayment_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      $result = array( 'success' => array(), 'err' => array());

      if( floatval($data['montoRestante']) == 0 ){
        okResponse("No existen cambios", "data", true, $this);
      }

      $restante = $this->db->query("SELECT SUM(montoParcial) as restante FROM cxc_payTable WHERE cxcId=".$data['cxcId']." AND payday > '".$data['payday']."'");
      $rest = $restante->row_array();

      if( $data['montoRestante'] + floatval($rest['restante']) < 0 ){
        errResponse('El monto a cobrar excede el monto restante. Favor de revisar detalle de pagos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', floatval($rest['restante']));
      }

      $q = $this->db->select('*')
              ->from('cxc_payTable')
              ->where( array( 'cxcId' => $data['cxcId'], 'status' => 1, 'payday >' => $data['payday']) )
              ->order_by('consecutivo')
              ->limit(1)
              ->get();
  
      $res = $q->row_array();

      if( floatval($data['montoRestante']) > 0 ){

        if( $q->num_rows() > 0 ){
          $monto = $data['montoRestante'] + $res['montoParcial'];
          if($this->db->set(array('montoParcial' => $monto, 'updater' => $_GET['usid']))
            ->where('id', $res['id'])->update('cxc_payTable')){
            array_push($result['success'], $res['id']);
          }else{
            array_push($result['err'], array('id' => $res['id'], 'err' => $this->db->error()));
          }

        }else{
          $q = $this->db->select('cxcId,
                                  montoFiscal,
                                  asesor,
                                  consecutivo + 1 AS consecutivo,
                                  maxParcialidad,
                                  montoParcial,
                                  1 AS status,
                                  IF(DAY(payday) = 15,
                                  LAST_DAY(payday),
                                  ADDDATE(payday, 15)) AS payday', FALSE)
            ->from('cxc_payTable')
            ->where( array( 'cxcId' => $data['cxcId'] ) )
            ->order_by('payday', 'DESC')
            ->limit(1)
            ->get();

          $res = $q->row_array();


          $monto = $data['montoRestante'];
          $res['montoParcial'] = $monto;

          if( $this->db->set($res)->set('updater', $_GET['usid'])
                ->insert('cxc_payTable') ){
                  array_push($result['success'], $this->db->insert_id());
          }else{
            array_push($result['err'], array('id' => 'nuevo', 'err' => $this->db->error()));
          }
        }

      }else{

        $data['montoRestante'] = floatval($data['montoRestante']) * (-1);

        for( $m = floatval($data['montoRestante']); $m > 0; $m = $m ){
          $q = $this->db->select('*')
              ->from('cxc_payTable')
              ->where( array( 'cxcId' => $data['cxcId'], 'status' => 1, 'payday >' => $data['payday']) )
              ->where('montoParcial > maxParcialidad',NULL, FALSE)
              ->order_by('payday')
              ->limit(1)
              ->get();
  
          $res = $q->row_array();
  
          if( $q->num_rows() > 0 ){
            
            if( $m > (floatval($res['montoParcial']-floatval($res['maxParcialidad']))) ){
              $monto = $res['maxParcialidad'];
              $m = $m - (floatval($res['montoParcial']-floatval($res['maxParcialidad'])));
              if( $m < .1 ){
                $m = 0;
              }
            }else{
              $monto = floatval($res['montoParcial'])-$m;
              $m = 0;
            }
  
            if($this->db->set(array('montoParcial' => $monto, 'updater' => $_GET['usid']))
              ->where('id', $res['id'])->update('cxc_payTable')){
                array_push($result['success'], $res['id']);
              }else{
                array_push($result['err'], array('id' => $res['id'], 'err' => $this->db->error()));
              }
          }else{

              $q = $this->db->select('*')
                  ->from('cxc_payTable')
                  ->where( array( 'cxcId' => $data['cxcId'], 'status' => 1, 'payday >' => $data['payday'], 'montoParcial >' => 0) )
                  ->order_by('payday', 'DESC')
                  ->limit(1)
                  ->get();
      
              $res = $q->row_array();
      
              if( $q->num_rows() > 0 ){
                
                if( $m > floatval($res['montoParcial']) ){
                  $monto = 0;
                  $m = $m - floatval($res['montoParcial']);
                  if( $m < .1 ){
                    $m = 0;
                  }
                }else{
                  $monto = floatval($res['montoParcial'])-$m;
                  $m = 0;
                }
      
                if($this->db->set(array('montoParcial' => $monto, 'updater' => $_GET['usid']))
                  ->where('id', $res['id'])->update('cxc_payTable')){
                    array_push($result['success'], $res['id']);
                  }else{
                    array_push($result['err'], array('id' => $res['id'], 'err' => $this->db->error()));
                  }
              }else{
                errResponse('Error en la integridad de los pagos generados', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', 'error');
              }
      
            
          }
  
        }

      }

      

      if( count($result['err']) == 0 ){

        $params = array('updater' => $_GET['usid'], 'montoParcial' => $data['nuevoMonto']);
        $this->db->set($params)
            ->where('id', $data['id']);
        
        if( $data['statusChange'] == 1 ){
          $this->db->set('status', $data['status']);
        }

        $this->db->update('cxc_payTable');

        okResponse("Pagos Editados", "data", true, $this);
      }else{
        errResponse('Error en la base de datos. '.count($result['err']).' errores encontrados', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $result['err'][0]['err']);
      }

      return $result;

    });

    $this->response( $result );

  }

  public function deactivate_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      $result = array( 'success' => array(), 'err' => array());

      if( floatval($data['montoRestante']) == 0 ){
        okResponse("No existen cambios", "data", true, $this);
      }

      if( floatval($data['montoRestante']) > 0 ){
        for( $m = floatval($data['montoRestante']); $m > 0; $m = $m ){
          $q = $this->db->select('*')
              ->from('cxc_payTable')
              ->where( array( 'cxcId' => $data['cxcId'], 'status' => 1, 'payday >' => $data['payday']) )
              ->where( 'montoParcial < maxParcialidad', NULL, FALSE )
              ->order_by('consecutivo')
              ->limit(1)
              ->get();
  
          $res = $q->row_array();
  
          if( $q->num_rows() > 0 ){
            $tmpMontoDisp = floatval($res['maxParcialidad'])-floatval($res['montoParcial']);
  
            if( $m > $tmpMontoDisp ){
              $monto = floatval($res['maxParcialidad']);
              $m = $m - $tmpMontoDisp;
              if( $m < .1 ){
                $m = 0;
              }
            }else{
              $monto = floatval($res['montoParcial'])+$m;
              $m = 0;
            }
  
            if($this->db->set(array('montoParcial' => $monto, 'updater' => $_GET['usid']))
              ->where('id', $res['id'])->update('cxc_payTable')){
                array_push($result['success'], $res['id']);
              }else{
                array_push($result['err'], array('id' => $res['id'], 'err' => $this->db->error()));
              }
          }else{
            $q = $this->db->select('cxcId,
                                    montoFiscal,
                                    asesor,
                                    consecutivo + 1 AS consecutivo,
                                    maxParcialidad,
                                    montoParcial,
                                    1 AS status,
                                    IF(DAY(payday) = 15,
                                    LAST_DAY(payday),
                                    ADDDATE(payday, 15)) AS payday', FALSE)
              ->from('cxc_payTable')
              ->where( array( 'cxcId' => $data['cxcId'] ) )
              ->order_by('payday', 'DESC')
              ->limit(1)
              ->get();
  
            $res = $q->row_array();
  
  
            if( $m > floatval($res['maxParcialidad']) ){
              $monto = floatval($res['maxParcialidad']);
              $m = $m - floatval($res['maxParcialidad']);
              if( $m < .1 ){
                $m = 0;
              }
            }else{
              $monto = $m;
              $m = 0;
            }
  
            $res['montoParcial'] = $monto;
  
            if( $this->db->set($res)->set('updater', $_GET['usid'])
                    ->insert('cxc_payTable') ){
                      array_push($result['success'], $this->db->insert_id());
              }else{
                array_push($result['err'], array('id' => 'nuevo', 'err' => $this->db->error()));
              }
          }
  
        }
      }else{
        $data['montoRestante'] = floatval($data['montoRestante']) * (-1);

        for( $m = floatval($data['montoRestante']); $m > 0; $m = $m ){
          $q = $this->db->select('*')
              ->from('cxc_payTable')
              ->where( array( 'cxcId' => $data['cxcId'], 'status' => 1, 'payday >' => $data['payday'], 'montoParcial >' => 0) )
              ->order_by('payday', 'DESC')
              ->limit(1)
              ->get();
  
          $res = $q->row_array();
  
          if( $q->num_rows() > 0 ){
            
            if( $m > floatval($res['montoParcial']) ){
              $monto = 0;
              $m = $m - floatval($res['montoParcial']);
              if( $m < .1 ){
                $m = 0;
              }
            }else{
              $monto = floatval($res['montoParcial'])-$m;
              $m = 0;
            }
  
            if($this->db->set(array('montoParcial' => $monto, 'updater' => $_GET['usid']))
              ->where('id', $res['id'])->update('cxc_payTable')){
                array_push($result['success'], $res['id']);
              }else{
                array_push($result['err'], array('id' => $res['id'], 'err' => $this->db->error()));
              }
          }else{
            errResponse('Error en la integridad de los pagos generados', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', 'error');
          }
  
        }

      }

      

      if( count($result['err']) == 0 ){

        $params = array('updater' => $_GET['usid'], 'montoParcial' => $data['nuevoMonto']);
        $this->db->set($params)
            ->where('id', $data['id']);
        
        if( $data['statusChange'] == 1 ){
          $this->db->set('status', $data['status']);
        }

        $this->db->update('cxc_payTable');

        okResponse("Pagos Editados", "data", true, $this);
      }else{
        errResponse('Error en la base de datos. '.count($result['err']).' errores encontrados', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $result['err'][0]['err']);
      }

      return $result;

    });

    $this->response( $result );

  }

  public function search_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS cxcAmount");
      $this->db->query("CREATE TEMPORARY TABLE cxcAmount
                        SELECT cxcIdLink, SUM(montoFiscal) as monto FROM cxc_transactions WHERE cxcIdLink IS NOT NULL GROUP BY cxcIdLink");
      $this->db->query("ALTER TABLE cxcAmount ADD PRIMARY KEY (cxcIdLink)");

      if( $data['noTx'] == true ){
        $this->db->query("DROP TEMPORARY TABLE IF EXISTS cxcRegs");
        $this->db->query("CREATE TEMPORARY TABLE cxcRegs
                          SELECT Localizador, COUNT(*) as regs FROM cxc_transactions GROUP BY Localizador");
        $this->db->query("ALTER TABLE cxcRegs ADD PRIMARY KEY (Localizador)");

        $this->db->join('asesores_cxc b', 'a.cxcIdLink=b.id', 'right')
              ->join('cxcRegs c', 'b.Localizador=c.Localizador', 'left')
              ->where('a.id IS NULL', '', FALSE);
      }else{

          $this->db->query("DROP TEMPORARY TABLE IF EXISTS cxcRegs");
          $this->db->query("CREATE TEMPORARY TABLE cxcRegs
                            SELECT Localizador, COUNT(*) as regs FROM asesores_cxc GROUP BY Localizador");
          $this->db->query("ALTER TABLE cxcRegs ADD PRIMARY KEY (Localizador)");
  
          $this->db->join('asesores_cxc b', 'a.cxcIdLink=b.id', 'left')
                ->join('cxcRegs c', 'a.Localizador=c.Localizador', 'left');
      }

      $this->db->select('a.id, COALESCE(a.Localizador, b.Localizador) as Localizador, dtCreated, montoFiscal, a.monto, currency, aplicablePara, 
                        autorizadoPor, cobradoPor, b.id as cxcIdLink, asesor as asesorID, NOMBREASESOR(asesor,1) as asesor, d.monto as totalCxc, 
                        status, tipo, firmado, comments, NOMBREASESOR(created_by,1) created_by, 
                        NOMBREASESOR(updated_by,1) updated_by, regs as posibleLinks')
        ->from('cxc_transactions a')
        ->join('cxcAmount d', 'b.id=d.cxcIdLink', 'left')
        ->order_by('dtCreated');

      if( $data['onlyReg'] == true ){
        $this->db->where('regs >',0);
      }

      switch( $data['type'] ){
        case 'Fecha':
          if( $data['noTx'] == true ){
            $this->db->where("fecha_cxc BETWEEN '".$data['search']['Fecha']['inicio']."' AND '".$data['search']['Fecha']['fin']."'", NULL, FALSE);
          }else{
            $this->db->where("dtCreated BETWEEN '".$data['search']['Fecha']['inicio']."' AND ADDDATE('".$data['search']['Fecha']['fin']."',1)", NULL, FALSE);
          }
          break;
        case 'Localizador':
          if( $data['noTx'] == true ){
            $this->db->where("b.Localizador", $data['search']['Localizador'], FALSE);
          }else{
            $this->db->where("a.Localizador", $data['search']['Localizador'], FALSE);
          }
          
          break;
        case 'asesor':
          $this->db->where("b.asesor", $data['search']['asesor'], FALSE);
          break;
      }

      if( $data['resumed'] == true ){
        $this->db->group_by('cxcIdLink')->having('cxcIdLink IS NOT ', 'NULL', FALSE);
      }

      
      if($q = $this->db->get()){
        
        $data = $q->result_array();
        // okResponse( 'CxC obtenidos', 'data', $data, $this );

        foreach($data as $index => $info){
          $url = $_SERVER['DOCUMENT_ROOT']."/img/cxc/".$info['cxcIdLink'].".jpg";

          if( file_exists( $url ) ){
            $data[$index]['Formato'] = 1;
          }else{
            $data[$index]['Formato'] = 0;
          }
        }

            okResponse( 'CxC obtenidos', 'data', $data, $this );

      }else{
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
      }

    });

    $this->response($result);

  }

  public function searchApply_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $dep = $this->uri->segment(3);

      $this->db->select('a.id, a.id as cxcIdLink, fecha_cxc as Fecha, a.Localizador, SUM(montoFiscal) as totalCxc, 
                          a.asesor as asesorID, NOMBREASESOR(a.asesor,1) as asesor, NOMBREDEP(dep) as Departamento,
                          status, tipo, firmado, comments, NOMBREASESOR(created_by,1) created_by, 
                        NOMBREASESOR(updated_by,1) updated_by')
        ->from('asesores_cxc a')
        ->join('cxc_transactions b', 'a.id=b.cxcIdLink', 'left')
        ->join('dep_asesores c', 'a.asesor=c.asesor AND c.Fecha=CURDATE()', 'left')
        ->where(array('a.status' => 1))
        ->group_by('a.id')
        ->having('totalCxc >', 0)
        ->order_by('asesor');

      if( isset($dep) ){
        if( $dep != 0 ){
          $this->db->where('dep', $dep);
        }else{
          $this->db->where('dep != ', 29);
        }
      }else{
        $this->db->where('dep != ', 29);
      }

      if($q = $this->db->get()){

        $data = $q->result_array();

        foreach($data as $index => $info){
          $url = $_SERVER['DOCUMENT_ROOT']."/img/cxc/".$info['id'].".jpg";

          if( file_exists( $url ) ){
            $data[$index]['Formato'] = 1;
          }else{
            $data[$index]['Formato'] = 0;
          }
        }

            okResponse( 'CxC obtenidos', 'data', $data, $this );

      }else{
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
      }

    });

    $this->response($result);

  }

  public function link_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $id_tx = $this->uri->segment(3);
      $id_cxc = $this->uri->segment(4);

      $this->db->where('id', $id_tx)
        ->set('cxcIdLink', $id_cxc);

      if($this->db->update('cxc_transactions')){
        $this->saveHistory($id_tx, $id_cxc, true, $_GET['usid']);
        okResponse( 'CxC ligado', 'data', true, $this );
      }else{
        errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
      }

    });

    $this->response($result);

  }

  public function unlink_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $id_tx = $this->uri->segment(3);
      $id_cxc = $this->uri->segment(4);

      $this->db->where('id', $id_tx)
        ->set('cxcIdLink', null);

      if($this->db->update('cxc_transactions')){
        $this->saveHistory($id_tx, $id_cxc, false, $_GET['usid']);
        okResponse( 'CxC ligado', 'data', true, $this );

      }else{
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
      }

    });

    $this->response($result);

  }

  public function new_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      $this->db->set($data)
        ->set('created_by', $_GET['usid'])
        ->set('monto', 0);

      if($this->db->insert('asesores_cxc')){

            okResponse( 'CxC Cargado', 'data', $this->db->insert_id(), $this );

      }else{
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
      }

    });

    $this->response($result);

  }

  public function editReg_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      $this->db->set($data['set'])
        ->set('updated_by', $_GET['usid'])
        ->where($data['where']);

      if($this->db->update('asesores_cxc')){

            okResponse( 'CxC Actualizado', 'data', true, $this );

      }else{
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
      }

    });

    $this->response($result);
  }

  public function apply_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();
      $summary = array( 'success' => array(), 'error' => array() );

      foreach($data as $index => $item){
        $this->db->set($item)
          ->set('maxParcialidad', $item['montoParcial'])
          ->set('updater', $_GET['usid']);

          if( $this->db->insert('cxc_payTable') ){
            array_push( $summary['success'], $item['consecutivo'] );
          }else{
            array_push( $summary['error'], array( 'cons' => $item['consecutivo'], 'err' => $this->db->error()) );
          }
          
      }

      if( count($summary['error']) == 0 ){
            $this->db->set( array('status' => 2, 'updated_by' => $_GET['usid']) )
                  ->where('id', $data[0]['cxcId'])
                  ->update('asesores_cxc');

            okResponse( 'Cxc Aplicado', 'data', true, $this );

      }else{
            $this->db->where( 'cxcId', $data[0]['cxcId'] )->delete('cxc_payTable');
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $summary['error'][0]);
      }

    });

    $this->response($result);
  }

  private function saveHistory($tx, $cxc, $link, $asesor){

    $data = array(
      'transactionId' => $tx,
      'cxcId' => $cxc,
      'link' => $link == true ? 1 : 0,
      'asesor' => $asesor
    );

    $this->db->set($data)->insert('cxc_linkHistory');
  }

}
