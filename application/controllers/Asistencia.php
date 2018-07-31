<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Asistencia extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('jwt');
    $this->load->helper('validators');
    $this->load->database();

  }

  public function pya_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $dep=$this->uri->segment(3);
      $inicio=$this->uri->segment(4);
      $fin=$this->uri->segment(5);
      $asesor = $this->uri->segment(6);
      $noSup = $this->uri->segment(7);
      $order = $this->uri->segment(8);

      $noSupFlag = isset($noSup) && $noSup == 1 ? true : false;
      $orderFlag = isset($order) && $order == 1 ? true : false;
        
        if( isset($asesor) && $asesor != 0 ){ 
            $isAsesor = "asesor IN (".str_replace('|', ',', $asesor).")";
            okResponse('success', 'data', $isAsesor, $this);
        }else{
            $isAsesor = "IF(@dep=0, dep != 29, dep = @dep)";
            $this->db->query("SET @dep = $dep");
        }

      $this->db->query("SET @inicio = CAST('$inicio' as DATE)");
      $this->db->query("SET @fin = CAST('$fin' as DATE)");
      
      $this->db->query("DROP TEMPORARY TABLE IF EXISTS asistenciaAsesores");
      $this->db->query("CREATE TEMPORARY TABLE asistenciaAsesores SELECT
          a.*,
          IF(vacante IS NOT NULL, num_colaborador, NULL) as Colaborador,
          IF(vacante IS NOT NULL, NOMBREDEP(dep), NULL) as Departamento,
          IF(vacante IS NOT NULL, NOMBREPUESTO(a.puesto), NULL) as PuestoName,
          esquema
      FROM
          dep_asesores a LEFT JOIN Asesores b ON a.asesor=b.id
      WHERE
          Fecha BETWEEN @inicio AND @fin AND vacante IS NOT NULL
              AND $isAsesor");
      $this->db->query("ALTER TABLE asistenciaAsesores ADD PRIMARY KEY (`Fecha`, `asesor`)");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS log_asesor");
      $this->db->query("CREATE TEMPORARY TABLE log_asesor (SELECT
          a.*,
      	b.id as h_id,
          js, je, x1s, x1e, x2s, x2e, cs, ce, phx,
          checkLog(a.Fecha, a.asesor, 'in') AS login,
          checkLog(a.Fecha, a.asesor, 'out') AS logout
      FROM
          asistenciaAsesores a
              LEFT JOIN
          asesores_programacion b ON a.asesor = b.asesor AND a.Fecha = b.Fecha)");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS xtraTime");
      $this->db->query("CREATE TEMPORARY TABLE xtraTime SELECT
      	a.Fecha, a.asesor,
      	IF(login IS NOT NULL,
          IF(login<=js AND logout>js,
            js,
            login
          ),
          NULL) as j_login,
        IF(logout IS NOT NULL,
          IF(logout>je AND login<=je,
            je,
            logout),
          NULL) as j_logout,

      	IF(x1s!=x1e,IF(login<x1e AND logout>=x1s,IF(login<x1s,x1s,login),NULL),NULL) as x1_login,
		IF(x1s!=x1e,IF(login<x1e AND logout>=x1s,IF(logout>x1e,x1e,logout),NULL),NULL) as x1_logout,
      	IF(x2s!=x2e,IF(login<x2e AND logout>=x2s,IF(login<x2s,x2s,login),NULL),NULL) as x2_login,
      	IF(x2s!=x2e,IF(login<x2e AND logout>=x2s,IF(logout>x2e,x2e,logout),NULL),NULL) as x2_logout
      FROM
      	log_asesor a");



      $this->db->query("DROP TEMPORARY TABLE IF EXISTS ausTable");
      $this->db->query("CREATE TEMPORARY TABLE ausTable SELECT
                  a.Fecha,
                  a.asesor,
                  b.id as ausentismoId,
                  b.ausentismo as Aus_id,
                  IF(js = je, 1, 0) AS Descanso,
                  CASE
                      WHEN login IS NULL THEN 0
                      WHEN login IS NOT NULL THEN 1
                  END AS Asistencia,
                  pdt,
                  CASE
                      WHEN b.ausentismo IS NULL THEN 0
                      ELSE 1
                  END AS Ausentismo,
                  caso as Aus_Caso,
                  comments as Aus_Nota,
                  changed_by as Aus_register,
                  b.Last_Update as Aus_LU, c.Ausentismo as Aus_Nombre,
                  CASE
                      WHEN b.ausentismo IS NOT NULL THEN
                          CASE
                              WHEN b.a = 1 THEN c.Code
                              WHEN b.d = 1 THEN 'D'
                              WHEN b.b = 1 THEN 'B'
                          END
                      ELSE NULL
                  END AS Code_aus,
                  IF(WEEKDAY(a.Fecha) + 1 = 7, 1, 0) AS Domingo
          FROM
              log_asesor a
          LEFT JOIN asesores_ausentismos b ON a.asesor = b.asesor
              AND a.Fecha = b.Fecha
          LEFT JOIN config_tiposAusentismos c ON b.ausentismo = c.id");

      $this->db->query("ALTER TABLE log_asesor ADD PRIMARY KEY (`Fecha`, `asesor`)");
      $this->db->query("ALTER TABLE xtraTime ADD PRIMARY KEY (`Fecha`, `asesor`)");
      $this->db->query("ALTER TABLE ausTable ADD PRIMARY KEY (`Fecha`, `asesor`)");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS pyaTable");
      $this->db->query("CREATE TEMPORARY TABLE pyaTable SELECT
                  Fecha, asesor,
                  tipo,
                  caso,
                  Nota,
                  Last_Update,
                  changed_by as reg_by,
                  Excepcion,
                  Codigo
          FROM
              asesores_pya_exceptions a
          LEFT JOIN config_tipos_pya_exceptions b ON a.tipo = b.id
          WHERE Fecha BETWEEN @inicio AND @fin");
      $this->db->query("ALTER TABLE pyaTable ADD PRIMARY KEY (Fecha, asesor)");


      $this->db->query("DROP TEMPORARY TABLE IF EXISTS asistenciaTableResult");
      $this->db->query("CREATE TEMPORARY TABLE asistenciaTableResult SELECT
      	NOMBREASESOR(a.asesor,2) as Nombre,
          a . *,
          j_login,
          j_logout,
          x1_login,
          x1_logout,
          x2_login,
          x2_logout,
          IF(Descanso=0 AND j_logout<je,1,0) as SalidaAnticipada,
          IF(Descanso=0,TIMESTAMPDIFF(SECOND,j_login,j_logout)/TIMESTAMPDIFF(SECOND,js,je)*100,null) as tiempoLaborado,
          CASE
              WHEN j_login > ADDTIME(js, '00:13:00') THEN 'RT-B'
              WHEN j_login >= ADDTIME(js, '00:01:00') THEN 'RT-A'
              ELSE NULL
          END as Retardo,
          CASE
              WHEN j_login >= ADDTIME(js, '00:01:00') THEN ADDTIME(j_login, - js)
              ELSE NULL
          END as Retardo_time,
          d.tipo as RT_tipo,
          d.caso as RT_caso,
          d.Nota as RT_Nota,
          d.Last_Update as RT_LU,
          NOMBREASESOR(d.reg_by,1) as RT_register,
          d.Excepcion as RT_Excepcion,
          d.Codigo as RT_Codigo,
          Descanso,
          Asistencia,
          ausentismoId,
          Ausentismo,
          Code_aus,
          Aus_caso, Aus_Nota, NOMBREASESOR(Aus_register,1) as Aus_Register, Aus_LU, Aus_Nombre, Aus_id,
          Domingo, pdt
      FROM
          log_asesor a
              LEFT JOIN
          xtraTime b ON a.Fecha = b.Fecha
              AND a.asesor = b.asesor
              LEFT JOIN
          ausTable c ON a.Fecha = c.Fecha
              AND a.asesor = c.asesor
              LEFT JOIN
          pyaTable d ON a.Fecha = d.Fecha AND a.asesor=d.asesor
      ORDER BY
          Fecha, Nombre");

          $q = $this->db->query("SELECT * FROM asistenciaTableResult");

      $result = $q->result_array();

      foreach($result as $index => $info){
        $fechas[$info['Fecha']]=1;
        $data[$info['asesor']]['Nombre']=$info['Nombre'];
        $data[$info['asesor']]['PuestoName']=$info['PuestoName'];
        $data[$info['asesor']]['Colaborador']=$info['Colaborador'];
        $data[$info['asesor']]['Departamento']=$info['Departamento'];
        $data[$info['asesor']]['data'][$info['Fecha']]=$info;
        unset($data[$info['asesor']]['data'][$info['Fecha']]['asesor']);
        unset($data[$info['asesor']]['data'][$info['Fecha']]['Nombre']);
        unset($data[$info['asesor']]['data'][$info['Fecha']]['PuestoName']);
        unset($data[$info['asesor']]['data'][$info['Fecha']]['Departamento']);
        unset($data[$info['asesor']]['data'][$info['Fecha']]['Colaborador']);
        unset($data[$info['asesor']]['data'][$info['Fecha']]['Fecha']);
      }

      return array('Fechas' => $fechas, 'data' => $data);

    });

    $this->response($result);


  }

  public function pya_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        $params = $this->put();

        $dep    = isset($params['dep']) ? $params['dep'] : null; 
        $inicio = isset($params['inicio']) ? $params['inicio'] : null; 
        $fin    = isset($params['fin']) ? $params['fin'] : null;
        $asesor = isset($params['asesor']) ? $params['asesor'] : null; 
        $noSup  = isset($params['noSup']) ? $params['noSup'] : null; 
        $order  = isset($params['order']) ? $params['order'] : null; 

        $noSupFlag = isset($noSup) && $noSup == 1 ? true : false;
        $orderFlag = isset($order) && $order == 1 ? true : false;
        
        if( isset($asesor) && $asesor != 0 ){ 
            $isAsesor = "asesor IN (";
            foreach($asesor as $index => $item){
                $isAsesor .= $item.",";
            }
            $isAsesor = substr($isAsesor,0,-1).")";
        }else{
            $isAsesor = "IF(@dep=0, dep != 29, dep = @dep)";
            $this->db->query("SET @dep = $dep");
        }

      $this->db->query("SET @inicio = CAST('$inicio' as DATE)");
      $this->db->query("SET @fin = CAST('$fin' as DATE)");
      
      $this->db->query("DROP TEMPORARY TABLE IF EXISTS asistenciaAsesores");
      $this->db->query("CREATE TEMPORARY TABLE asistenciaAsesores SELECT
          a.*,
          IF(vacante IS NOT NULL, num_colaborador, NULL) as Colaborador,
          IF(vacante IS NOT NULL, NOMBREDEP(dep), NULL) as Departamento,
          IF(vacante IS NOT NULL, NOMBREPUESTO(a.puesto), NULL) as PuestoName,
          esquema
      FROM
          dep_asesores a LEFT JOIN Asesores b ON a.asesor=b.id
      WHERE
          Fecha BETWEEN @inicio AND @fin AND vacante IS NOT NULL
              AND $isAsesor");
      $this->db->query("ALTER TABLE asistenciaAsesores ADD PRIMARY KEY (`Fecha`, `asesor`)");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS log_asesor");
      $this->db->query("CREATE TEMPORARY TABLE log_asesor (SELECT
          a.*,
      	b.id as h_id,
          js, je, x1s, x1e, x2s, x2e, cs, ce, phx,
          checkLog(a.Fecha, a.asesor, 'in') AS login,
          checkLog(a.Fecha, a.asesor, 'out') AS logout
      FROM
          asistenciaAsesores a
              LEFT JOIN
          asesores_programacion b ON a.asesor = b.asesor AND a.Fecha = b.Fecha)");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS xtraTime");
      $this->db->query("CREATE TEMPORARY TABLE xtraTime SELECT
      	a.Fecha, a.asesor,
      	IF(login IS NOT NULL,
          IF(login<=js AND logout>js,
            js,
            login
          ),
          NULL) as j_login,
        IF(logout IS NOT NULL,
          IF(logout>je AND login<=je,
            je,
            logout),
          NULL) as j_logout,

      	IF(x1s!=x1e,IF(login<x1e AND logout>=x1s,IF(login<x1s,x1s,login),NULL),NULL) as x1_login,
		IF(x1s!=x1e,IF(login<x1e AND logout>=x1s,IF(logout>x1e,x1e,logout),NULL),NULL) as x1_logout,
      	IF(x2s!=x2e,IF(login<x2e AND logout>=x2s,IF(login<x2s,x2s,login),NULL),NULL) as x2_login,
      	IF(x2s!=x2e,IF(login<x2e AND logout>=x2s,IF(logout>x2e,x2e,logout),NULL),NULL) as x2_logout
      FROM
      	log_asesor a");



      $this->db->query("DROP TEMPORARY TABLE IF EXISTS ausTable");
      $this->db->query("CREATE TEMPORARY TABLE ausTable SELECT
                  a.Fecha,
                  a.asesor,
                  b.id as ausentismoId,
                  b.ausentismo as Aus_id,
                  IF(js = je, 1, 0) AS Descanso,
                  CASE
                      WHEN login IS NULL THEN 0
                      WHEN login IS NOT NULL THEN 1
                  END AS Asistencia,
                  pdt,
                  CASE
                      WHEN b.ausentismo IS NULL THEN 0
                      ELSE 1
                  END AS Ausentismo,
                  caso as Aus_Caso,
                  comments as Aus_Nota,
                  changed_by as Aus_register,
                  b.Last_Update as Aus_LU, c.Ausentismo as Aus_Nombre,
                  CASE
                      WHEN b.ausentismo IS NOT NULL THEN
                          CASE
                              WHEN b.a = 1 THEN c.Code
                              WHEN b.d = 1 THEN 'D'
                              WHEN b.b = 1 THEN 'B'
                          END
                      ELSE NULL
                  END AS Code_aus,
                  IF(WEEKDAY(a.Fecha) + 1 = 7, 1, 0) AS Domingo
          FROM
              log_asesor a
          LEFT JOIN asesores_ausentismos b ON a.asesor = b.asesor
              AND a.Fecha = b.Fecha
          LEFT JOIN config_tiposAusentismos c ON b.ausentismo = c.id");

      $this->db->query("ALTER TABLE log_asesor ADD PRIMARY KEY (`Fecha`, `asesor`)");
      $this->db->query("ALTER TABLE xtraTime ADD PRIMARY KEY (`Fecha`, `asesor`)");
      $this->db->query("ALTER TABLE ausTable ADD PRIMARY KEY (`Fecha`, `asesor`)");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS pyaTable");
      $this->db->query("CREATE TEMPORARY TABLE pyaTable SELECT
                  Fecha, asesor,
                  tipo,
                  caso,
                  Nota,
                  Last_Update,
                  changed_by as reg_by,
                  Excepcion,
                  Codigo
          FROM
              asesores_pya_exceptions a
          LEFT JOIN config_tipos_pya_exceptions b ON a.tipo = b.id
          WHERE Fecha BETWEEN @inicio AND @fin");
      $this->db->query("ALTER TABLE pyaTable ADD PRIMARY KEY (Fecha, asesor)");


      $this->db->query("DROP TEMPORARY TABLE IF EXISTS asistenciaTableResult");
      $this->db->query("CREATE TEMPORARY TABLE asistenciaTableResult SELECT
      	NOMBREASESOR(a.asesor,2) as Nombre,
          a . *,
          j_login,
          j_logout,
          x1_login,
          x1_logout,
          x2_login,
          x2_logout,
          IF(Descanso=0 AND j_logout<je,1,0) as SalidaAnticipada,
          IF(Descanso=0,TIMESTAMPDIFF(SECOND,j_login,j_logout)/TIMESTAMPDIFF(SECOND,js,je)*100,null) as tiempoLaborado,
          CASE
              WHEN j_login > ADDTIME(js, '00:13:00') THEN 'RT-B'
              WHEN j_login >= ADDTIME(js, '00:01:00') THEN 'RT-A'
              ELSE NULL
          END as Retardo,
          CASE
              WHEN j_login >= ADDTIME(js, '00:01:00') THEN ADDTIME(j_login, - js)
              ELSE NULL
          END as Retardo_time,
          d.tipo as RT_tipo,
          d.caso as RT_caso,
          d.Nota as RT_Nota,
          d.Last_Update as RT_LU,
          NOMBREASESOR(d.reg_by,1) as RT_register,
          d.Excepcion as RT_Excepcion,
          d.Codigo as RT_Codigo,
          Descanso,
          Asistencia,
          ausentismoId,
          Ausentismo,
          Code_aus,
          Aus_caso, Aus_Nota, NOMBREASESOR(Aus_register,1) as Aus_Register, Aus_LU, Aus_Nombre, Aus_id,
          Domingo, pdt
      FROM
          log_asesor a
              LEFT JOIN
          xtraTime b ON a.Fecha = b.Fecha
              AND a.asesor = b.asesor
              LEFT JOIN
          ausTable c ON a.Fecha = c.Fecha
              AND a.asesor = c.asesor
              LEFT JOIN
          pyaTable d ON a.Fecha = d.Fecha AND a.asesor=d.asesor
      ORDER BY
          Fecha, Nombre");

          $q = $this->db->query("SELECT * FROM asistenciaTableResult");

      $result = $q->result_array();

      foreach($result as $index => $info){
        $fechas[$info['Fecha']]=1;
        $data[$info['asesor']]['Nombre']=$info['Nombre'];
        $data[$info['asesor']]['PuestoName']=$info['PuestoName'];
        $data[$info['asesor']]['Colaborador']=$info['Colaborador'];
        $data[$info['asesor']]['Departamento']=$info['Departamento'];
        $data[$info['asesor']]['data'][$info['Fecha']]=$info;
        unset($data[$info['asesor']]['data'][$info['Fecha']]['asesor']);
        unset($data[$info['asesor']]['data'][$info['Fecha']]['Nombre']);
        unset($data[$info['asesor']]['data'][$info['Fecha']]['PuestoName']);
        unset($data[$info['asesor']]['data'][$info['Fecha']]['Departamento']);
        unset($data[$info['asesor']]['data'][$info['Fecha']]['Colaborador']);
        unset($data[$info['asesor']]['data'][$info['Fecha']]['Fecha']);
      }

      return array('Fechas' => $fechas, 'data' => $data);

    });

    $this->response($result);


  }

public function pyaV2_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $dep=$this->uri->segment(3);
      $inicio=$this->uri->segment(4);
      $fin=$this->uri->segment(5);
      $asesor = $this->uri->segment(6);
      $noSup = $this->uri->segment(7);
      $order = $this->uri->segment(8);

      $noSupFlag = isset($noSup) && $noSup == 1 ? true : false;
      $orderFlag = isset($order) && $order == 1 ? true : false;

      if($noSupFlag){
          $isSup = "AND a.puesto IN (1,2)";
        }else{
            $isSup = "";
      }

      if($orderFlag){
          $orderPos = "Fecha, posicion";
        }else{
            $orderPos = "Fecha, Nombre ";
      }
        
        if( isset($asesor) && $asesor > 0 ){ 
            $isAsesor = "asesor IN ($asesor)";
        }else{
            $isAsesor = "IF(@dep=0, dep != 29, dep = @dep)";
            $this->db->query("SET @dep = $dep");
        }

      $this->db->query("SET @inicio = CAST('$inicio' as DATE)");
      $this->db->query("SET @fin = CAST('$fin' as DATE)");
      
      $this->db->query("DROP TEMPORARY TABLE IF EXISTS asistenciaAsesores");
      $this->db->query("CREATE TEMPORARY TABLE asistenciaAsesores SELECT
          a.*,
          IF(vacante IS NOT NULL, num_colaborador, NULL) as Colaborador,
          IF(vacante IS NOT NULL, NOMBREDEP(dep), NULL) as Departamento,
          IF(vacante IS NOT NULL, NOMBREPUESTO(a.puesto), NULL) as PuestoName,
          esquema
      FROM
          dep_asesores a LEFT JOIN Asesores b ON a.asesor=b.id
      WHERE
          Fecha BETWEEN @inicio AND @fin AND vacante IS NOT NULL
              AND $isAsesor $isSup");
      $this->db->query("ALTER TABLE asistenciaAsesores ADD PRIMARY KEY (`Fecha`, `asesor`)");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS log_asesor");
      $this->db->query("CREATE TEMPORARY TABLE log_asesor (SELECT
          a.*,
      	b.id as h_id,
          js, je, x1s, x1e, x2s, x2e, cs, ce, phx,
          checkLog(a.Fecha, a.asesor, 'in') AS login,
          checkLog(a.Fecha, a.asesor, 'out') AS logout
      FROM
          asistenciaAsesores a
              LEFT JOIN
          asesores_programacion b ON a.asesor = b.asesor AND a.Fecha = b.Fecha)");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS xtraTime");
      $this->db->query("CREATE TEMPORARY TABLE xtraTime SELECT
      	a.Fecha, a.asesor,
      	IF(login IS NOT NULL,
          IF(login<=js AND logout>js,
            js,
            login
          ),
          NULL) as j_login,
        IF(logout IS NOT NULL,
          IF(logout>je AND login<=je,
            je,
            logout),
          NULL) as j_logout,

      	IF(x1s!=x1e,IF(login<x1e AND logout>=x1s,IF(login<x1s,x1s,login),NULL),NULL) as x1_login,
		IF(x1s!=x1e,IF(login<x1e AND logout>=x1s,IF(logout>x1e,x1e,logout),NULL),NULL) as x1_logout,
      	IF(x2s!=x2e,IF(login<x2e AND logout>=x2s,IF(login<x2s,x2s,login),NULL),NULL) as x2_login,
      	IF(x2s!=x2e,IF(login<x2e AND logout>=x2s,IF(logout>x2e,x2e,logout),NULL),NULL) as x2_logout
      FROM
      	log_asesor a");



      $this->db->query("DROP TEMPORARY TABLE IF EXISTS ausTable");
      $this->db->query("CREATE TEMPORARY TABLE ausTable SELECT
                  a.Fecha,
                  a.asesor,
                  b.id as ausentismoId,
                  b.ausentismo as Aus_id,
                  IF(js = je, 1, 0) AS Descanso,
                  CASE
                      WHEN login IS NULL THEN 0
                      WHEN login IS NOT NULL THEN 1
                  END AS Asistencia,
                  pdt,
                  CASE
                      WHEN b.ausentismo IS NULL THEN 0
                      ELSE 1
                  END AS Ausentismo,
                  caso as Aus_Caso,
                  comments as Aus_Nota,
                  changed_by as Aus_register,
                  b.Last_Update as Aus_LU, c.Ausentismo as Aus_Nombre,
                  CASE
                      WHEN b.ausentismo IS NOT NULL THEN
                          CASE
                              WHEN b.a = 1 THEN c.Code
                              WHEN b.d = 1 THEN 'D'
                              WHEN b.b = 1 THEN 'B'
                          END
                      ELSE NULL
                  END AS Code_aus,
                  IF(WEEKDAY(a.Fecha) + 1 = 7, 1, 0) AS Domingo
          FROM
              log_asesor a
          LEFT JOIN asesores_ausentismos b ON a.asesor = b.asesor
              AND a.Fecha = b.Fecha
          LEFT JOIN config_tiposAusentismos c ON b.ausentismo = c.id");

      $this->db->query("ALTER TABLE log_asesor ADD PRIMARY KEY (`Fecha`, `asesor`)");
      $this->db->query("ALTER TABLE xtraTime ADD PRIMARY KEY (`Fecha`, `asesor`)");
      $this->db->query("ALTER TABLE ausTable ADD PRIMARY KEY (`Fecha`, `asesor`)");

      $this->db->query("DROP TEMPORARY TABLE IF EXISTS pyaTable");
      $this->db->query("CREATE TEMPORARY TABLE pyaTable SELECT
                  Fecha, asesor,
                  tipo,
                  caso,
                  Nota,
                  Last_Update,
                  changed_by as reg_by,
                  Excepcion,
                  Codigo
          FROM
              asesores_pya_exceptions a
          LEFT JOIN config_tipos_pya_exceptions b ON a.tipo = b.id
          WHERE Fecha BETWEEN @inicio AND @fin");
      $this->db->query("ALTER TABLE pyaTable ADD PRIMARY KEY (Fecha, asesor)");


      $this->db->query("DROP TEMPORARY TABLE IF EXISTS asistenciaTableResult");
      $this->db->query("CREATE TEMPORARY TABLE asistenciaTableResult SELECT
      	NOMBREASESOR(a.asesor,2) as Nombre,
          a . *,
          j_login,
          j_logout,
          x1_login,
          x1_logout,
          x2_login,
          x2_logout,
          IF(Descanso=0 AND j_logout<je,1,0) as SalidaAnticipada,
          IF(Descanso=0,TIMESTAMPDIFF(SECOND,j_login,j_logout)/TIMESTAMPDIFF(SECOND,js,je)*100,null) as tiempoLaborado,
          CASE
              WHEN j_login > ADDTIME(js, '00:13:00') THEN 'RT-B'
              WHEN j_login >= ADDTIME(js, '00:01:00') THEN 'RT-A'
              ELSE NULL
          END as Retardo,
          CASE
              WHEN j_login >= ADDTIME(js, '00:01:00') THEN ADDTIME(j_login, - js)
              ELSE NULL
          END as Retardo_time,
          d.tipo as RT_tipo,
          d.caso as RT_caso,
          d.Nota as RT_Nota,
          d.Last_Update as RT_LU,
          NOMBREASESOR(d.reg_by,1) as RT_register,
          d.Excepcion as RT_Excepcion,
          d.Codigo as RT_Codigo,
          Descanso,
          Asistencia,
          ausentismoId,
          Ausentismo,
          Code_aus,
          Aus_caso, Aus_Nota, NOMBREASESOR(Aus_register,1) as Aus_Register, Aus_LU, Aus_Nombre, Aus_id,
          Domingo, pdt, posicion
      FROM
          log_asesor a
              LEFT JOIN
          xtraTime b ON a.Fecha = b.Fecha
              AND a.asesor = b.asesor
              LEFT JOIN
          ausTable c ON a.Fecha = c.Fecha
              AND a.asesor = c.asesor
              LEFT JOIN
          pyaTable d ON a.Fecha = d.Fecha AND a.asesor=d.asesor
              LEFT JOIN
          horarios_position_select e ON a.asesor=e.asesor AND WEEK(a.Fecha,1)=e.semana AND YEAR(a.Fecha)=e.year
      ORDER BY
          $orderPos");

          $q = $this->db->query("SELECT * FROM asistenciaTableResult");

      $result = $q->result_array();

      foreach($result as $index => $info){
        $fechas[$info['Fecha']]=1;
        $data[$info['asesor']]['Nombre']=$info['Nombre'];
        $data[$info['asesor']]['PuestoName']=$info['PuestoName'];
        $data[$info['asesor']]['Colaborador']=$info['Colaborador'];
        $data[$info['asesor']]['Departamento']=$info['Departamento'];
        $data[$info['asesor']]['data'][$info['Fecha']]=$info;
        unset($data[$info['asesor']]['data'][$info['Fecha']]['asesor']);
        unset($data[$info['asesor']]['data'][$info['Fecha']]['Nombre']);
        unset($data[$info['asesor']]['data'][$info['Fecha']]['PuestoName']);
        unset($data[$info['asesor']]['data'][$info['Fecha']]['Departamento']);
        unset($data[$info['asesor']]['data'][$info['Fecha']]['Colaborador']);
        unset($data[$info['asesor']]['data'][$info['Fecha']]['Fecha']);
      }
      $comida = $this->db->query("SELECT comida FROM dep_asesores WHERE Fecha=ADDDATE(ADDDATE(CURDATE(),-WEEKDAY(CURDATE())),7) AND asesor=$asesor");
      $cRow = $comida->row_array();
      okResponse('Horarios obtenidos', 'data', $data, $this, 'array', array('array' => $result, 'comida' => $cRow['comida'] ));

    });


  }

  public function ausPorAsesor_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $asesor   = $this->uri->segment(3);
      $fecha    = $this->uri->segment(4);

      $ausentismo = $this->db->query("SELECT
                            *, DATEDIFF(Fin, Inicio)+1 - Descansos - Beneficios as dias, motivo
                        FROM
                            Ausentismos a
                              LEFT JOIN
                            `Dias Pendientes Redimidos` b ON a.ausent_id = b.id_ausentismo
                        WHERE
                            asesor = $asesor
                                AND '$fecha' BETWEEN Inicio AND Fin");

      $result = $ausentismo->row();

      if($result == NULL){
        $result = 0;
      }

      return $result;

    });

    $this->response($result);


  }

  public function tipos_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $asesor   = $this->uri->segment(3);

      $ausentismos = $this->db->query("SELECT
                            id, Ausentismo, max_days as dias
                        FROM
                            `Tipos Ausentismos`
                        ORDER BY
                            Ausentismo");

      $result = $ausentismos->result_array();

      $pendientes = $this->diasPendientes($asesor);

      return array("tipos" => $result, "pending" => $pendientes);

    });

    $this->response($result);

  }

  public function diasPendientes( $asesor ){

    $query = $this->db->query("SELECT
                          a.id as asesor,
                          a.motivo,
                          assign,
                          IF(redim IS NULL, 0, redim) as redim,
                          assign - IF(redim IS NULL, 0, redim) as available
                      FROM
                          (SELECT
                              id, sum(`dias asignados`) as assign, motivo
                          FROM
                              `Dias Pendientes`
                          GROUP BY id , motivo) a
                              LEFT JOIN
                          (SELECT
                              id, sum(dias) as redim, motivo
                          FROM
                              `Dias Pendientes Redimidos`
                          GROUP BY id , motivo) b ON a.motivo = b.motivo AND a.id = b.id
                      WHERE a.id=$asesor HAVING available>0 ORDER BY a.motivo");

      return $query->result_array();

  }

  public function setAusentismo_put(){

    $data = $this->put();
    $this->load->library('form_validation');
    $this->form_validation->set_data( $data );

    if( $this->form_validation->run( 'ausentismo_put' ) ){
      $this->response( $data );
    }else{
      errResponse( "Existen errores en el formulario", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->form_validation->get_errores_arreglo() );
    }



    // $this->response( $data );

  }

  public function calendario_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $inicio = $this->uri->segment(3);
      $fin    = $this->uri->segment(4);
      $skill  = $this->uri->segment(5);

      segmentSet( 3, 'El segmento 3 debe incluir la fecha de inicio', $this );
      segmentSet( 4, 'El segmento 4 debe incluir la fecha final', $this );
      segmentSet( 5, 'El segmento 5 debe incluir el skill que se busca', $this );
      segmentType( 3, 'El formato debe ser de tipo fecha YYYY-MM-DD', $this, 'date' );
      segmentType( 4, 'El formato debe ser de tipo fecha YYYY-MM-DD', $this, 'date' );
      segmentType( 5, 'El formato debe ser el id del skill', $this );

      $this->db->query("SET @inicio = CAST('$inicio' as DATE)");
      $this->db->query("SET @fin = CAST('$fin' as DATE)");
      $this->db->query("SET @skill = $skill");

//      $this->db->query("DROP TEMPORARY TABLE IF EXISTS aus_registry");
//
//      $this->db->select("a.*,
//                        Code,
//                        Ausentismo,
//                        showcal, d.clave")
//              ->select(" NOMBREASESOR(a.asesor, 1) AS nombre ", FALSE)
//              ->from("Ausentismos a")
//              ->join("dep_asesores b", "a.asesor = b.asesor AND a.Inicio = b.Fecha", 'left')
//              ->join("`Tipos Ausentismos` c ", "a.tipo_ausentismo = c.id", 'left', FALSE)
//              ->join("hc_codigos_Puesto d", "b.hc_puesto = d.id", 'left')
//              ->where("Fin >= @inicio
//                        AND Inicio <= @fin
//                        AND vacante IS NOT NULL
//                        AND dep = @skill
//
//                        AND showcal = 1", NULL, FALSE);
//
//      $ausReg = $this->db->get_compiled_select();
        
        $this->db->select("a.*,
                        Code,
                        a.Ausentismo,
                        showcal, d.clave")
              ->select(" NOMBREASESOR(a.asesor, 1) AS nombre ", FALSE)
              ->from("asesores_ausentismos a")
              ->join("dep_asesores b", "a.asesor = b.asesor AND a.Fecha = b.Fecha", 'left')
              ->join("config_tiposAusentismos c ", "a.ausentismo = c.id", 'left', FALSE)
              ->join("hc_codigos_Puesto d", "b.hc_puesto = d.id", 'left')
              ->where("a.Fecha BETWEEN @inicio AND @fin
                        AND vacante IS NOT NULL
                        AND dep = @skill
                        AND puesto != 11
                        AND showcal = 1", NULL, FALSE);


      if( $aus = $this->db->get() ){

        $this->db->select("a.Fecha,
                            espacios,
                            abierto")
                  ->select("COUNT(IF(b.id IS NOT NULL AND clave LIKE '%d%',
                                b.id,
                                NULL)) AS asignados,
                            espacios - COUNT(IF(b.id IS NOT NULL AND clave LIKE '%d%',
                                b.id,
                                NULL)) AS disponibles", FALSE)
                  ->from("ausentismos_calendario a")
                  ->join("asesores_ausentismos b", "a.Fecha = b.Fecha", "left")
                  ->join("dep_asesores c", "b.asesor = c.asesor
                                            AND b.Fecha = c.Fecha
                                            AND a.Departamento = c.dep AND c.vacante IS NOT NULL", "left")
                  ->join("hc_codigos_Puesto d", "c.hc_puesto = d.id", "left")
                  ->join("config_tiposAusentismos e", "b.ausentismo = e.id", "left")
                  ->where("a.Departamento = @skill AND a.Fecha BETWEEN @inicio AND @fin", NULL, FALSE)
                  ->where("(showcal = 1 OR showcal IS NULL)", NULL, FALSE)
                  ->group_by("a.Fecha");

        if( $q = $this->db->get() ){
          okResponse( 'Ausentismos Obtenidos', 'data', array('aus' => $aus->result_array(), 'q' => $q->result_array() ), $this );
        }else{
          errResponse( "Error en la base de datos Cantidades", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error() );
        }

      }else{
        errResponse( "Error en la base de datos Ausentismos", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error(), 'query', $ausReg );
      }

      return true;

    });

    $this->response( $result );

  }
    
  public function tiposAusentismos_get( ){
      
      $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        if( $q = $this->db->query("SELECT * FROM `Tipos Ausentismos` ORDER BY Ausentismo") ){
            okResponse( 'Tipos Obtenidos', 'data', $q->result_array(), $this );
        }else{
          errResponse( "Error en la base de datos Cantidades", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error() );
        }

        });
      
      return true;
  }
    
  public function validateDates_put( ){
      
      $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
          
        $data = $this->put();
        
        $asesor = $data['asesor'];
        $dates  = $data['dates'];
          
        $this->db->select('*')
                ->from('asesores_ausentismos')
                ->where('asesor', $asesor)
                ->where_in('Fecha', $dates);

        if( $q = $this->db->get() ){
            okResponse( 'Fechas Validadas', 'data', $q->num_rows(), $this );
        }else{
          errResponse( "Error en la base de datos Cantidades", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error() );
        }

        });
      
      return true;
  }
    
  public function saveAus_put( ){
      
      $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
          
        $d = $this->put();
          
        $data = $d['data'];
        $motive = $d['motivo'];
        $days = $d['dias'];
        
        if( $q = $this->db->insert_batch('asesores_ausentismos', $data, FALSE) ){
            
            $lastId = $this->db->query("SELECT LAST_INSERT_ID() as id FROM asesores_ausentismos LIMIT 1");
            $id = $lastId->row_array();
            
            $id['m']=$id['id'];
            
            $this->db->query("UPDATE asesores_ausentismos SET id=".$id['id']." WHERE id >= ".$id['id']."");
            $this->db->query("ALTER TABLE asesores_ausentismos AUTO_INCREMENT = ".($id['id']++));
            
            if( $data[0]['ausentismo'] == 5 ){
                
                $dr = array( 'pdt_paid' => 1 );
                
                $this->db->set($dr)
                    ->where(array('id' => $id['m'], 'a' => 1))
                    ->update("asesores_ausentismos");
                
            }

            
            okResponse( 'Ausentismos Guardados', 'data', true, $this );
        }else{
          errResponse( "Error en la base de datos Cantidades", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error() );
        }

        });
      
      return true;
  }
    
  public function diasPendientes_get( ){
      
      $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
          
        $asesor = $this->uri->segment(3);
          
        $query = "SELECT 
                    a.asesor,
                    a.motivo,
                    dias_asignados,
                    IF(dias_redimidos IS NULL,
                        0,
                        dias_redimidos) AS dias_redimidos,
                    dias_asignados - IF(dias_redimidos IS NULL,
                        0,
                        dias_redimidos) AS dias_pendientes
                FROM
                    (SELECT 
                        id AS asesor,
                            SUM(`dias asignados`) AS dias_asignados,
                            motivo
                    FROM
                        `Dias Pendientes`
                    WHERE
                        id = $asesor
                    GROUP BY motivo , asesor) a
                        LEFT JOIN
                    (SELECT 
                        id AS asesor, SUM(`dias`) AS dias_redimidos, motivo
                    FROM
                        `Dias Pendientes Redimidos`
                    WHERE
                        id = $asesor
                    GROUP BY motivo , asesor) b ON a.asesor = b.asesor
                        AND a.motivo = b.motivo
                HAVING dias_pendientes > 0";
        
        if( $q = $this->db->query($query) ){
            
            okResponse( 'Dias Pendientes Obtenidos', 'data', $q->result_array(), $this, 'num', $q->num_rows() );
        }else{
          errResponse( "Error en la base de datos Cantidades", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error() );
        }

        });
      
      return true;
  }
    
  public function tiposExc_get( ){
      
      $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
          
        $type = $this->uri->segment(3);
        $showAll = $this->uri->segment(4);
          
        if( $type == 'false' ){
            $query = "SELECT id, Excepcion as name, Codigo as Code FROM config_tipos_pya_exceptions ORDER BY name"; 
        }else{
            $this->db->select('id, Ausentismo as name, Code')
                    ->from('config_tiposAusentismos')
                    ->order_by('name');
            
            if( $showAll == 'false' ){
                $this->db->where('showPya', 1);
            }
            
            $query = $this->db->get_compiled_select();
        }
          
        
        if( $q = $this->db->query($query) ){
            
            okResponse( 'Tipos Obtenidos', 'data', $q->result_array(), $this );
        }else{
          errResponse( "Error en la base de datos de tipos", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error() );
        }

        });
      
      return true;
  }
    
    public function registeredExc_get( ){
      
      $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
          
        $asesor = $this->uri->segment(3);
        $fecha = $this->uri->segment(4);
          
         if($aus = $this->db->select('id, ausentismo as tipo, caso, comments as Nota, Last_Update, NOMBREASESOR(changed_by,1) as changed', FALSE)
            ->from('asesores_ausentismos')
            ->where('asesor', $asesor)
            ->where('Fecha', $fecha)
            ->get() ){
             
             if( $aus->num_rows() > 0){
                 okResponse( 'Tipos Obtenidos', 'data', $aus->row_array(), $this, 'exc', 1 );
             }else{
                 if($rt = $this->db->select('id, tipo, caso, Nota, Last_Update, NOMBREASESOR(changed_by,1) as changed', FALSE)
                                ->from('asesores_pya_exceptions')
                                ->where('asesor', $asesor)
                                ->where('Fecha', $fecha)
                    ->get() ){

                     if( $rt->num_rows() > 0){
                         okResponse( 'Tipos Obtenidos', 'data', $rt->row_array(), $this, 'exc', 2 );
                     }else{
                        okResponse( 'Tipos Obtenidos', 'data', $rt->row_array(), $this, 'exc', 0 );
                     }
                 }else{
                     errResponse( "Error en la base de datos de tipos", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error() );
                 }
             }
         }else{
             errResponse( "Error en la base de datos de tipos", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error() );
         }

        });
      
      return true;
  }
    
    public function saveExc_put( ){
      
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
          
        $data = $this->put();
          
        
        // VALIDATE AUS EXCEPTIONS
        if($aus = $this->db->select('a.*, showPya', FALSE)
            ->from('asesores_ausentismos a')
            ->join('config_tiposAusentismos b', 'a.ausentismo = b.id', 'LEFT')
            ->where('asesor', $data['form']['asesor'])
            ->where( 'Fecha', $data['form']['Fecha'] )
            ->get() ){
              
            if( $aus->num_rows() > 0){
                $ausData = $aus->row_array();
                
                if( $ausData['showPya'] == 1 ){
                    
                    if( $data['tipo'] == 'true' ){
                        
                        $this->db->where('asesor', $data['form']['asesor'])
                                ->where( 'Fecha', $data['form']['Fecha'] )
                                ->set($data['form']);
                        
                        if( $this->db->update('asesores_ausentismos') ){
                            okResponse( 'Excepción Actualizada', 'data', true, $this, 'tipo', 1);
                        }else{
                            errResponse( "Error al actualizar excepción", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error() );
                        }
                    }else{
                        $this->db->where('asesor', $data['form']['asesor'])
                                ->where( 'Fecha', $data['form']['Fecha'] );
                        
                        if( !$this->db->delete('asesores_ausentismos') ){
                            errResponse( "Error al eliminar excepción existente", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error() );
                        }
                    }
                    
                }else{
                    errResponse( "No es posible sobreescribir un Ausentismo programado", REST_Controller::HTTP_BAD_REQUEST, $this, 'Existente', $ausData );
                }
                
            }
        }else{
            errResponse( "No es posible consultar las excepciones, no se actualizó nada", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error() );
        }
            
        // VALIDATE RT EXCEPTIONS
        if($rt = $this->db->select('*')
            ->from('asesores_pya_exceptions')
            ->where('asesor', $data['form']['asesor'])
            ->where( 'Fecha', $data['form']['Fecha'] )
            ->get() ){
              
            if( $rt->num_rows() > 0){
                $rtData = $rt->row_array();
                
                if( $data['tipo'] == 'true' ){

                   $this->db->where('asesor', $data['form']['asesor'])
                            ->where( 'Fecha', $data['form']['Fecha'] );

                    if( !$this->db->delete('asesores_pya_exceptions') ){
                        errResponse( "Error al eliminar excepción existente", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error() );
                    }
                    
                }else{
                    
                    $this->db->where('asesor', $data['form']['asesor'])
                                ->where( 'Fecha', $data['form']['Fecha'] )
                                ->set($data['form']);
                        
                        if( $this->db->update('asesores_pya_exceptions') ){
                            okResponse( 'Excepción Actualizada', 'data', true, $this, 'tipo', 0);
                        }else{
                            errResponse( "Error al actualizar excepción", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error() );
                        }
                    
                }

            }
        }else{
            errResponse( "No es posible consultar las excepciones, no se actualizó nada", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error() );
        }
            
        $this->db->where('asesor', $data['form']['asesor'])
                            ->where( 'Fecha', $data['form']['Fecha'] )
                            ->set($data['form']);
        
        if( $data['tipo'] == 'true' ){
            $table = "asesores_ausentismos";
            $this->db->set( array('a' => 1));
            $flag = 1;
        }else{
            $table = "asesores_pya_exceptions";
            $flag = 0;
        }
            
        if( $this->db->insert($table) ){
            okResponse( 'Excepción Actualizada', 'data', true, $this, 'tipo', $flag);
        }else{
            errResponse( "Error al actualizar excepción", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error() );
        }            
        
            
        

    });
      
    return true;
  }
    
    public function excDelete_put( ){
      
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
          
        $data = $this->put();
          
        
        // VALIDATE AUS EXCEPTIONS
        if($aus = $this->db->select('a.*, showPya', FALSE)
            ->from('asesores_ausentismos a')
            ->join('config_tiposAusentismos b', 'a.ausentismo = b.id', 'LEFT')
            ->where('asesor', $data['form']['asesor'])
            ->where( 'Fecha', $data['form']['Fecha'] )
            ->get() ){
              
            if( $aus->num_rows() > 0){
                $ausData = $aus->row_array();
                
                if( $ausData['showPya'] == 1 ){
                     
                    $this->db->where('asesor', $data['form']['asesor'])
                            ->where( 'Fecha', $data['form']['Fecha'] );

                    if( $this->db->delete('asesores_ausentismos') ){
                        okResponse( 'Excepción Eliminada', 'data', true, $this);
                    }else{
                        errResponse( "Error al actualizar excepción", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error() );
                    }

                    
                }else{
                    errResponse( "No es posible eliminar un Ausentismo programado", REST_Controller::HTTP_BAD_REQUEST, $this, 'Existente', $ausData );
                }
                
            }
        }else{
            errResponse( "No es posible consultar las excepciones, no se actualizó nada", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error() );
        }
            
        // VALIDATE RT EXCEPTIONS
        if($rt = $this->db->select('*')
            ->from('asesores_pya_exceptions')
            ->where('asesor', $data['form']['asesor'])
            ->where( 'Fecha', $data['form']['Fecha'] )
            ->get() ){
              
            if( $rt->num_rows() > 0){
                $rtData = $rt->row_array();
                
               $this->db->where('asesor', $data['form']['asesor'])
                        ->where( 'Fecha', $data['form']['Fecha'] );

                if( $this->db->delete('asesores_pya_exceptions') ){
                    okResponse( 'Excepción Eliminada', 'data', true, $this);
                }else{
                    errResponse( "Error al eliminar excepción existente", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error() );
                }
                    
            }
        }else{
            errResponse( "No es posible consultar las excepciones, no se actualizó nada", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error() );
        }
            
        errResponse( "Nada que eliminar", REST_Controller::HTTP_BAD_REQUEST, $this );

    });
      
    return true;
  }
    
    public function horarioAsesor_get( ){
      
      $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
          
       $asesor = $this->uri->segment(3);
       $inicio = $this->uri->segment(4);
       $fin = $this->uri->segment(5);

        $this->db->select('a.*, c.Ausentismo, d.comida')
                ->select('NOMBREASESOR(a.asesor,1) as nombre')
                ->from('asesores_programacion a')
                ->join('asesores_ausentismos b', 'a.asesor = b.asesor AND a.Fecha = b.Fecha', 'left')
                ->join('config_tiposAusentismos c', 'b.ausentismo = c.id', 'left')
                ->join('dep_asesores d', 'a.asesor = d.asesor AND ADDDATE(a.Fecha,7)=d.Fecha', 'left')
                ->where('a.asesor', $asesor)
                ->order_by('a.Fecha');

        if( isset($inicio) ){
            $this->db->where('a.Fecha BETWEEN ', "'$inicio' AND '$fin'", FALSE);
        }else{
            $this->db->where('a.Fecha BETWEEN ', 'CURDATE() AND ADDDATE(CURDATE(), 7)', FALSE);
        }
          
        if( $q = $this->db->get() ){
            
            $comida = $this->db->query("SELECT comida FROM dep_asesores WHERE Fecha=ADDDATE(ADDDATE(CURDATE(),-WEEKDAY(CURDATE())),7) AND asesor=$asesor");
            $cRow = $comida->row_array();
            okResponse( 'Horarios obtenidos', 'data', $q->result_array(), $this, 'comida', $cRow['comida'] );
        }else{
          errResponse( "Error en la base de datos Cantidades", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error() );
        }

        });
      
      return true;
  }
    
    public function asesorAusentismos_get( ){
      
      $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
          
       $asesor = $this->uri->segment(3);

        $this->db->select('a.id, a.ausentismo AS tipo, CONCAT(b.Ausentismo,\'<br>\',NOMBREASESOR(changed_by,1),\' (\',Last_Update, \')\') as Ausentismo', FALSE)
            ->select("a.caso")
            ->select("MIN(Fecha) AS Inicio, MAX(Fecha) AS Fin, SUM(a) AS Dias, SUM(b) AS Ben, SUM(d) AS Des", FALSE)
            ->from('asesores_ausentismos a')
            ->join('config_tiposAusentismos b', 'a.ausentismo = b.id', 'LEFT')
            ->where('asesor', $asesor)
            ->group_by('a.id')
            ->order_by('Inicio');

        if( $q = $this->db->get() ){
            
            okResponse( 'Ausentismos obtenidos', 'data', $q->result_array(), $this );
        }else{
          errResponse( "Error en la base de datos", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error() );
        }

        });
      
      return true;
  }
    
    public function delAusentismos_delete( ){
      
      $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
          
       $id = $this->uri->segment(3);

        $this->db->where('id', $id);

        if( $q = $this->db->delete('asesores_ausentismos') ){
            
            $this->db->query("DELETE FROM `Dias Pendientes Redimidos` WHERE id_ausentismo = $id");
            
            okResponse( 'Ausentismo Eliminado', 'data', true, $this );
        }else{
          errResponse( "Error en la base de datos", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error() );
        }

        });
      
      return true;
  } 
    
    public function changeComida_put( ){
      
      $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
          
       $data = $this->put();

        $this->db->where('Fecha >= ', 'ADDDATE(ADDDATE(CURDATE(),-WEEKDAY(CURDATE())),7)', FALSE)
            ->where('asesor', $data['asesor'])
            ->set('comida', intVal($data['comida']));

        if( $q = $this->db->update('dep_asesores') ){
            
            if( intVal($data['comida']) == 0 ){
                $oldval = 1;
            }else{
                $oldval = 0;
            }
            
            $this->db->set('asesor', $data['asesor'])
                    ->set('campo', 'comida')
                    ->set('old_val', $oldval)
                    ->set('new_val', intVal($data['comida']))
                    ->set('changed_by', $_GET['usid'])
                    ->set('date ','NOW()', FALSE );
            $this->db->insert('historial_asesores');
            
            okResponse( 'Comidas Actualizadas', 'data', true, $this );
        }else{
          errResponse( "Error en la base de datos", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error() );
        }

        });
      
      return true;
  }
    
    
    public function chgHxPayment_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        $update = $this->put();
        
        $q = $this->db->query("SELECT IF('".$update['fecha']."'<inicio,0,1) as valid FROM comeycom_WFM.rrhh_calendarioNomina WHERE CURDATE() BETWEEN inicio AND fin;");
        $qRes = $q->row_array();

        if( $qRes['valid'] == 0 ){
            errResponse( "Corte de nómina pasado, esta fecha ya se envió a prenómina y no es posible modificarlo", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', FALSE );
        }
        
        $params = array('phx' => intVal($update['phx']));
        
        $this->db->where('id', $update['horario'])
            ->set($params);

      if( $this->db->update('asesores_programacion') ){
        okResponse( 'Tipo de pago Actualizado', 'data', true, $this );

      }else{
        errResponse( "Error en la base de datos Ausentismos", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error(), 'query', $ausReg );
      }

      return true;

    });

    $this->response( $result );
}

    public function chgDtPayment_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        $update = $this->put();
        
        $q = $this->db->query("SELECT IF('".$update['fecha']."'<inicio,0,1) as valid FROM rrhh_calendarioNomina WHERE CURDATE() BETWEEN inicio AND fin");
        $qRes = $q->row_array();

        if( $qRes['valid'] == 0 ){
            errResponse( "Corte de nómina pasado, esta fecha ya se envió a prenómina y no es posible modificarlo", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', FALSE );
        }
        
        $params = array('pdt' => intVal($update['phx']));
        
        $this->db->where('id', $update['horario'])
            ->set($params);

      if( $this->db->update('asesores_ausentismos') ){
        okResponse( 'Tipo de pago Actualizado', 'data', true, $this );

      }else{
        errResponse( "Error en la base de datos Ausentismos", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error(), 'query', $ausReg );
      }

      return true;

    });

    $this->response( $result );

  }
    
    public function logoutHorarios_get(){
        
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            if($q = $this->db->query("SELECT 
                                a.Fecha,
                                IF(c.Ausentismo IS NOT NULL,
                                    c.Ausentismo,
                                    IF(js IS NULL,
                                        'Sin Captura',
                                        IF(js = je,
                                            'Descanso',
                                            CONCAT(IF(HOUR(js) < 10, '0', ''),
                                                    HOUR(js),
                                                    ':',
                                                    IF(MINUTE(js) < 10, '0', ''),
                                                    MINUTE(js),
                                                    ' - ',
                                                    IF(HOUR(je) < 10, '0', ''),
                                                    HOUR(je),
                                                    ':',
                                                    IF(MINUTE(je) < 10, '0', ''),
                                                    MINUTE(je))))) AS jornada,
                                IF(x1s IS NULL,
                                    '',
                                    IF(x1s = x1e,
                                        '',
                                        CONCAT(IF(HOUR(x1s) < 10, '0', ''),
                                                HOUR(x1s),
                                                ':',
                                                IF(MINUTE(x1s) < 10, '0', ''),
                                                MINUTE(x1s),
                                                ' - ',
                                                IF(HOUR(x1e) < 10, '0', ''),
                                                HOUR(x1e),
                                                ':',
                                                IF(MINUTE(x1e) < 10, '0', ''),
                                                MINUTE(x1e)))) AS x1,
                                IF(x2s IS NULL,
                                    '',
                                    IF(x2s = x2e,
                                        '',
                                        CONCAT(IF(HOUR(x2s) < 10, '0', ''),
                                                HOUR(x2s),
                                                ':',
                                                IF(MINUTE(x2s) < 10, '0', ''),
                                                MINUTE(x2s),
                                                ' - ',
                                                IF(HOUR(x2e) < 10, '0', ''),
                                                HOUR(x2e),
                                                ':',
                                                IF(MINUTE(x2e) < 10, '0', ''),
                                                MINUTE(x2e)))) AS x2,
                                IF(cs IS NULL,
                                    '',
                                    IF(cs = ce,
                                        '',
                                        CONCAT(IF(HOUR(cs) < 10, '0', ''),
                                                HOUR(cs),
                                                ':',
                                                IF(MINUTE(cs) < 10, '0', ''),
                                                MINUTE(cs),
                                                ' - ',
                                                IF(HOUR(ce) < 10, '0', ''),
                                                HOUR(ce),
                                                ':',
                                                IF(MINUTE(ce) < 10, '0', ''),
                                                MINUTE(ce)))) AS c
                            FROM
                                asesores_programacion a
                                    LEFT JOIN
                                asesores_ausentismos b ON a.asesor = b.asesor
                                    AND a.Fecha = b.Fecha
                                    LEFT JOIN
                                config_tiposAusentismos c ON b.ausentismo = c.id
                            WHERE
                                a.asesor = 31
                                    AND a.Fecha BETWEEN ADDDATE(CURDATE(), 1) AND ADDDATE(CURDATE(), 2);")){
                
            okResponse( 'Horarios logout', 'data', $q->result_array(), $this );

          }else{
            errResponse( "Error en la base de datos Ausentismos", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error(), 'query', $ausReg );
          }

          return true;

        });

        $this->response( $result );
    }

    public function schedulesEditList_put(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $params = $this->put();

            $this->db->select("a.asesor, a.Fecha, b.id, js, je, x1s, x1e, x2s, x2e, cs, ce, NOMBREASESOR(a.asesor, 2) AS Nombre, IF(d.id IS NOT NULL, CASE WHEN c.a = 1 THEN d.Ausentismo WHEN c.b THEN 'Beneficio' WHEN c.d THEN 'Descanso' END,NULL) as aus", FALSE)
            ->from("dep_asesores a")
            ->join("asesores_programacion b", "a.asesor=b.asesor AND a.Fecha=b.Fecha", 'left')
            ->join("asesores_ausentismos c", "a.asesor=c.asesor AND a.Fecha=c.Fecha", 'left')
            ->join("config_tiposAusentismos d", "c.ausentismo=d.id", 'left')
            ->where_in('a.asesor', $params['asesores'])
            ->where('a.Fecha >=', $params['inicio'] )
            ->where('a.Fecha <=', $params['fin'] )
            ->order_by('Nombre')
            ->order_by('b.Fecha');

            if( $q = $this->db->get() ){
                okResponse( 'Programación recibida', 'data', $q->result_array(), $this );
            }else{
                errResponse( "Error en la base de datos", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $this->db->error() );
            }
        
            return true;
    
        });
    }
    
    public function schedulesChargeSave_put(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $data = $this->put();

            $error = array();

            foreach($data as $item => $info){
                $insert = array( 
                    'Fecha' => $info['Fecha'], 
                    'asesor' => $info['asesor'], 
                    'jornada start' => $info['js'] == null ? null : date("H:i", strtotime($info['js'])).":00", 
                    'jornada end' => $info['je'] == null ? null : date("H:i", strtotime($info['je'])).":00", 
                    'extra1 start' => $info['x1s'] == null ? null : date("H:i", strtotime($info['x1s'])).":00", 
                    'extra1 end' => $info['x1e'] == null ? null : date("H:i", strtotime($info['x1e'])).":00", 
                    'extra2 start' => $info['x2s'] == null ? null : date("H:i", strtotime($info['x2s'])).":00", 
                    'extra2 end' => $info['x2e'] == null ? null : date("H:i", strtotime($info['x2e'])).":00", 
                    'comida start' => $info['cs'] == null ? null : date("H:i", strtotime($info['cs'])).":00", 
                    'comida end' => $info['ce'] == null ? null : date("H:i", strtotime($info['ce'])).":00"
                );
                $upd = " ON DUPLICATE KEY UPDATE ";
                $fields = "(";
                $values = "(";
                
                foreach($insert as $key => $field){
                    $value = $field == null ? "NULL" : "'$field'";
                    $upd .= "`$key` = $value, ";

                    $fields .= "`$key`, ";
                    $values .= "$value, ";
                }

                $upd = substr($upd,0,-2);
                $fields = substr($fields,0,-2).")";
                $values = substr($values,0,-2).")";

                $ins = "INSERT INTO `Historial Programacion` $fields VALUES $values";
                $query = $ins.$upd;
                if(!$this->db->query($query)){
                    array_push($error, array( 'Fecha' => $info['Fecha'], 'asesor' => $info['asesor'] ) );
                }

                $nUpd = " ON DUPLICATE KEY UPDATE ";
                foreach($info as $key => $field){
                    if( $key != 'asesor' && $key != 'Fecha' && $key != 'id' ){
                        $value = $field == null ? "NULL" : "'$field'";
                        $nUpd .= "`$key` = $value, ";
                    }
                }
                $nUpd = substr($nUpd,0,-2);

                $nIns = $this->db->set($info)->get_compiled_insert('asesores_programacion');
                $nQ = $nIns.$nUpd;
                $this->db->query($nQ);
            }

            if( count( $error ) == 0 ){
                okResponse( count($data).' elemento(s) guardados', 'data', true, $this );
            }else{
                errResponse( "Error en la base de datos. ".count( $error )." elementos con errores", REST_Controller::HTTP_BAD_REQUEST, $this, 'errores', $errores );
            }
        
            return true;
    
        });
    }
    
    
}
