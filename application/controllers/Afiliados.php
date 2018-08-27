<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Afiliados extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->database();
  }

  public function afiliadosList_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        $query = "SELECT 
                    a.id,
                    afiliado,
                    IF(b.view = 1 OR afiliados_all = 1
                            OR allmighty = 1,
                        1,
                        0) AS flag
                FROM
                    afiliados_reportes a
                        LEFT JOIN
                    afiliados_permisos b ON a.id = b.report AND b.user = ".$_GET['usid']."
                        LEFT JOIN
                    userDB c ON c.asesor_id = ".$_GET['usid']."
                        LEFT JOIN
                    profilesDB d ON d.id = c.profile
                HAVING flag = 1";
          
      if( $q = $this->db->query($query) ){  
          
        if( $q->num_rows() == 0 ){
            errResponse('No tienes reportes habilitados. Por favor contacta a WFM para solicitar ayuda', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', 'Sin reportes asignados');
        }
        okResponse( 'Información Obtenida', 'data', $q->result_array(), $this );
      }else{
        errResponse('Error al obtener listado', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
      }

    });

  }

  public function reporte_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        $report = $this->uri->segment(3);
        $inicio = $this->uri->segment(4);
        $fin = $this->uri->segment(5);

        segmentSet( 3, 'Número de reporte es mandatorio', $this );
        segmentSet( 4, 'Fecha de inicio necesaria', $this );
        segmentSet( 5, 'Fecha de fin necesaria', $this );

        // ===============================================================
        // START validación de permisos
        // ===============================================================
            $qVal = $this->db->query("SELECT 
                                        IF( allmighty = 1 OR afiliados_all = 1 OR c.view = 1, 1, 0) as license
                                    FROM
                                        userDB a
                                            LEFT JOIN
                                        profilesDB b ON a.profile = b.id
                                            LEFT JOIN
                                        afiliados_permisos c ON a.asesor_id = c.user AND c.report = $report
                                            AND c.view = 1
                                    WHERE
                                        asesor_id = ".$_GET['usid']);
            $rVal = $qVal->row_array();

            if ( $rVal['license'] == 0 ) {
                errResponse( 'Permiso denegado para reporte '.$report, REST_Controller::HTTP_BAD_REQUEST, $this );
            }
        // ===============================================================
        // END validación de permisos
        // ===============================================================

        // ===============================================================
        // START obtención de parámetros
        // ===============================================================
            $pQuery = "SELECT * FROM afiliados_reportes WHERE id=$report";
            if( $pq = $this->db->query($pQuery) ){
                $params = $pq->row_array();
            }else{
                errResponse( 'Error al parametrizar reporte '.$report, REST_Controller::HTTP_BAD_REQUEST, $this );
            }

        // ===============================================================
        // END obtención de parámetros
        // ===============================================================

        // ===============================================================
        // START query de Afiliados
        // ===============================================================
      
            $this->generalKpis($inicio, $fin, $params);
            $query = "SELECT * FROM result";
            $queryTotal = "SELECT 'Total' as Fecha, 
                                SUM(MontoIn) as MontoIn,
                                SUM(MontoOut) as MontoOut,
                                SUM(MontoCC) as MontoCC,
                                SUM(MontoOnline) as MontoOnline,
                                SUM(MontoAll) as MontoAll,
                                SUM(LocsIn) as LocsIn,
                                SUM(LocsOut) as LocsOut,
                                SUM(LocsOnline) as LocsOnline,
                                SUM(LocsCC) as LocsCC,
                                SUM(LocsAll) as LocsAll,
                                IF(SUM(LocsIn) = 0, 0, SUM(MontoIn) / SUM(LocsIn)) as AvTktIn,
                                IF(SUM(LocsOut) = 0, 0, SUM(MontoOut) / SUM(LocsOut)) as AvTktOut,
                                IF(SUM(LocsOnline) = 0, 0, SUM(MontoOnline) / SUM(LocsOnline)) as AvTktOnline,
                                IF(SUM(LocsCC) = 0, 0, SUM(MontoCC) / SUM(LocsCC)) as AvTktCC,
                                IF(SUM(LocsAll) = 0, 0, SUM(MontoAll) / SUM(LocsAll)) as AvTktAll,
                                IF(SUM(Contestadas) = 0, 0, SUM(LocsIn) / SUM(Contestadas)) as FC,
                                SUM(Llamadas) as Llamadas,
                                SUM(Contestadas) as Contestadas,
                                SUM(Contestadas_all) as Contestadas_all,
                                SUM(Abandonadas) as Abandonadas,
                                IF(SUM(Llamadas) = 0, 0, SUM(Abandonadas) / SUM(Llamadas)) as Abandon,
                                SUM(Sla_Calls) as Sla_Calls,
                                IF(SUM(Llamadas) = 0, 0, SUM(Sla_Calls) / SUM(Llamadas)) as SLA,
                                SUM(Total_Espera) as Total_Espera,
                                IF(SUM(Contestadas_all) = 0, 0, SUM(Total_Espera) / (SUM(Contestadas_all))) as ASA,
                                SUM(TT) as TT,
                                IF(SUM(Contestadas_all) = 0, 0, SUM(TT) / (SUM(Contestadas_all))) as AHT
                            FROM result";

        // ===============================================================
        // END query de Afiliados
        // ===============================================================
        
        
        if( $q = $this->db->query($query) ){    
            $qT = $this->db->query($queryTotal);
            okResponse( 'Información Obtenida', 'data', $q->result_array(), $this, 'total', $qT->row_array() );
        }else{
            errResponse('Error al obtener reporte personalizado', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

    });

  }

  private function generalKpis($inicio, $fin, $params){

    $this->db->query("SET @inicio = CAST('$inicio' AS DATE)");
    $this->db->query("SET @fin = CAST('$fin' AS DATE)");
    
    $this->db->query("DROP TEMPORARY TABLE IF EXISTS afLocs");
    $this->db->query("CREATE TEMPORARY TABLE afLocs SELECT 
        a.Fecha, Localizador, a.asesor, c.dep,
        SUM(IF(gpoTipoRsva != 'Out'  AND a.asesor >= 0, VentaMXN + OtrosIngresosMXN + EgresosMXN,0)) AS MontoIn,
        SUM(IF(gpoTipoRsva != 'Out'  AND a.asesor =-1, VentaMXN + OtrosIngresosMXN + EgresosMXN,0)) AS MontoOnline,
        SUM(IF(gpoTipoRsva = 'Out', VentaMXN + OtrosIngresosMXN + EgresosMXN,0)) AS MontoOut,
        IF(gpoTipoRsva != 'Out'  AND a.asesor >= 0, IF(SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) > 0
                AND SUM(VentaMXN) > 0,
            Localizador,
            NULL),NULL) AS NewLocIn,
        IF(gpoTipoRsva != 'Out'  AND a.asesor =-1, IF(SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) > 0
                AND SUM(VentaMXN) > 0,
            Localizador,
            NULL),NULL) AS NewLocOnline,
        IF(gpoTipoRsva = 'Out', IF(SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) > 0
                AND SUM(VentaMXN) > 0,
            Localizador,
            NULL),NULL) AS NewLocOut, gpoTipoRsva
    FROM
        t_Locs a
            LEFT JOIN
        chanGroups b ON a.chanId = b.id
            LEFT JOIN
        dep_asesores c ON a.asesor = c.asesor
            AND a.Fecha = c.Fecha
            LEFT JOIN
        config_tipoRsva d ON IF(c.dep IS NULL, IF(a.asesor = - 1, - 1, 0), IF(c.dep NOT IN (0 , 3, 5, 29, 35, 50, 52), 0, c.dep)) = d.dep
                                                AND IF(a.tipo IS NULL OR a.tipo='',0, a.tipo) = d.tipo
    WHERE
        a.chanId IN (".$params['chanIds'].")
            AND a.Fecha BETWEEN @inicio AND @fin
    GROUP BY a.Fecha, Localizador");
    
    $this->db->query("DROP TEMPORARY TABLE IF EXISTS afCallsRAW");
    $this->db->query("CREATE TEMPORARY TABLE afCallsRAW
    SELECT 
        a.*, Skill, direction
    FROM
        t_Answered_Calls a
            LEFT JOIN
        Cola_Skill b ON a.Cola = b.Cola 
    WHERE 
        a.Fecha BETWEEN @inicio AND @fin AND 
        DNIS IN (".$params['dids'].")
    HAVING Skill IN (".$params['skills'].") AND direction = 1");
    
    $this->db->query("DROP TEMPORARY TABLE IF EXISTS afCalls");
    $this->db->query("CREATE TEMPORARY TABLE afCalls
    SELECT 
        Fecha,
        COUNT(*) as Ofrecidas,
        COUNT(IF(Answered = 1
                AND NOT (Desconexion = 'Transferida'
                AND Duracion_Real < '02:00:00'),
            ac_id,
            NULL)) AS Contestadas,
        COUNT(IF(Answered = 1, ac_id,NULL)) AS Contestadas_all,
        COUNT(IF(Answered = 0,ac_id,NULL)) as Abandonadas,
        COUNT(IF(Answered = 1 AND Espera <'00:00:".$params['tat']."',ac_id,NULL)) as Sla_Calls,
        SUM(IF(Answered = 1, TIME_TO_SEC(Espera), 0)) as Total_Espera,
        SUM(IF(Answered = 1
        AND NOT (Desconexion = 'Transferida'
        AND Duracion_Real < '02:00:00'),TIME_TO_SEC(Duracion_Real),NULL)) as TT
        FROM
        afCallsRAW
        GROUP BY Fecha");
        $this->db->query("ALTER TABLE afCalls ADD PRIMARY KEY (Fecha)");
        
    $this->db->query("DROP TEMPORARY TABLE IF EXISTS result");
    $this->db->query("CREATE TEMPORARY TABLE result SELECT 
        f.Fecha,
        COALESCE(MontoIn,0) as MontoIn,
        COALESCE(MontoOut,0) MontoOut,
        COALESCE(MontoOut,0) + COALESCE(MontoIn,0) as MontoCC,
        COALESCE(MontoOnline,0) MontoOnline,
        COALESCE(MontoOut,0) + COALESCE(MontoIn,0) + COALESCE(MontoOnline,0) as MontoAll,
        COALESCE(LocsIn,0) as LocsIn,
        COALESCE(LocsOut,0) as LocsOut,
        COALESCE(LocsOnline,0) as LocsOnline,
        COALESCE(LocsOut,0) + COALESCE(LocsIn,0) AS LocsCC,
        COALESCE(LocsOut,0) + COALESCE(LocsIn,0) + COALESCE(LocsOnline,0)AS LocsAll,
        IF(COALESCE(LocsIn,0) = 0, 0, COALESCE(MontoIn,0) / COALESCE(LocsIn,0)) AS AvTktIn,
        IF(COALESCE(LocsOut,0) = 0, 0, COALESCE(MontoOut,0) / COALESCE(LocsOut,0)) AS AvTktOut,
        IF(COALESCE(LocsOnline,0) = 0, 0, COALESCE(MontoOnline,0) / COALESCE(LocsOnline,0)) AS AvTktOnline,
        IF((COALESCE(LocsIn,0) + COALESCE(LocsOut,0)) = 0, 0, (COALESCE(MontoOut,0) + COALESCE(MontoIn,0)) / (COALESCE(LocsIn,0) + COALESCE(LocsOut,0))) AS AvTktCC,
        IF((COALESCE(LocsIn,0) + COALESCE(LocsOut,0) + COALESCE(LocsOnline,0)) = 0, 0, (COALESCE(MontoOut,0) + COALESCE(MontoIn,0) + COALESCE(MontoOnline,0)) / (COALESCE(LocsIn,0) + COALESCE(LocsOut,0) + COALESCE(LocsOnline,0))) AS AvTktAll,
        IF(COALESCE(Contestadas_all,0) = 0, 0, COALESCE(LocsIn,0) / COALESCE(Contestadas_all,0)) AS FC,
        COALESCE(Ofrecidas,0) as Llamadas,
        COALESCE(Contestadas,0) as Contestadas,
        COALESCE(Contestadas_all,0) as Contestadas_all,
        COALESCE(Abandonadas,0) as Abandonadas,
        IF(COALESCE(Ofrecidas,0) = 0,0,COALESCE(Abandonadas,0) / COALESCE(Ofrecidas,0)) as Abandon,
        COALESCE(Sla_Calls,0) as Sla_Calls,
        IF(COALESCE(Ofrecidas,0) = 0,0,COALESCE(Sla_Calls,0) / COALESCE(Ofrecidas,0)) AS SLA,
        COALESCE(Total_Espera,0) as Total_Espera,
        IF(COALESCE(Contestadas_all,0) = 0,0,COALESCE(Total_Espera,0) / (COALESCE(Contestadas_all,0))) AS ASA,
        COALESCE(TT,0) as TT,
        IF(COALESCE(Contestadas_all,0) = 0,0,COALESCE(TT,0) / (COALESCE(Contestadas_all,0))) AS AHT
    FROM
        (SELECT 
            *
        FROM
            Fechas
        WHERE
            Fecha BETWEEN @inicio AND @fin) f
            LEFT JOIN
        (SELECT 
            Fecha,
                SUM(MontoIn) AS MontoIn,
                COUNT(DISTINCT NewLocIn) AS LocsIn,
                SUM(MontoOut) AS MontoOut,
                COUNT(DISTINCT NewLocOut) AS LocsOut,
                SUM(MontoOnline) AS MontoOnline,
                COUNT(DISTINCT NewLocOnline) AS LocsOnline
        FROM
            afLocs
        GROUP BY Fecha) a ON f.Fecha = a.Fecha
            LEFT JOIN
        afCalls b ON f.Fecha = b.Fecha");
  }
 
}
