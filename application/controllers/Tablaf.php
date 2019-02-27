<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
date_default_timezone_set('America/Mexico_city');
// use REST_Controller;


class Tablaf extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('base_venta');
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->database();
  }

  public function mp_put(){
    
    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        // =================================================
        // START Params
        // =================================================
            $inicio = $this->uri->segment(3);
            $fin = $this->uri->segment(4);
            $skill = $this->uri->segment(5);
            
            // Incluye o excluye el canal de agencias
            if($skill == 7){
                $ag = true;
            }else{
                $ag = false;
            }
            
            // Incluye a todos los paises para MT
            if($skill == 3){
                $pais = "";
            }else{
                $pais = "AND pais != 'CO'";
            }

            // Definición de parámetros por skill
            // $params = array(
            //     '3'     => array( 'skin' => '3', 'skout' => '52', 'skill' => "(3,52)", 'marca' => "'Marcas Terceros'", 'mp' => false ),
            //     '7'     => array( 'skin' => '7', 'skout' => '7', 'skill' => "(7)", 'marca' => "'Marcas Terceros'", 'mp' => false ),
            //     '8'     => array( 'skin' => '8', 'skout' => '8', 'skill' => "(8)", 'marca' => "'Marcas Terceros'", 'mp' => false ),
            //     '9'     => array( 'skin' => '9', 'skout' => '9', 'skill' => "(9)", 'marca' => "'Marcas Terceros'", 'mp' => false ),
            //     '4'     => array( 'skin' => '4', 'skout' => '4', 'skill' => "(4)", 'marca' => "'Marcas Terceros'", 'mp' => false ),
            //     '35'    => array( 'skin' => '35', 'skout' => '5', 'skill' => "(35,5,50)", 'marca' => "'Marcas Propias'", 'mp' => true )
            // );
            $params = $this->put();
            if( $params['mp'] == 1 ){
                $params['mp'] = true;
            }else{
                $params['mp'] = false;
            }
        // =================================================
        // END Params
        // =================================================

        // =================================================
        // START Definición de variables para query
        // =================================================
            $this->db->query("SET @inicio = CAST('$inicio' as DATE)");
            $this->db->query("SET @fin = CAST('$fin' as DATE)");
            $this->db->query("SET @marca = '".$params['marca']."'");
        // =================================================
        // END Definición de variables para query
        // =================================================

        // =================================================
        // START Query Locs
        // =================================================
            venta_help::ventaF($this, $inicio, $fin, false, false, $skill, $params, $ag);
        // =================================================
        // END Query Locs
        // =================================================

        // =================================================
        // START Query KPIs Venta
        // =================================================
            $this->db->query("DROP TEMPORARY TABLE IF EXISTS kpisVenta");
            $query = "SELECT 
                        Fecha,
                        Skill,
                        Grupo,
                        SUM(monto) AS monto,
                        SUM(monto_hotel) AS monto_hotel,
                        SUM(monto_tour) AS monto_tour,
                        SUM(monto_transfer) AS monto_transfer,
                        SUM(monto_vuelo) AS monto_vuelo,
                        SUM(monto_seguro) AS monto_seguro,
                        SUM(RNs) AS RN,
                        SUM(margen) AS margen,
                        SUM(IF(servicio = 'Hotel',margen,0)) AS margen_hotel,
                        COUNT(DISTINCT CountLocOut) AS LocsOut,
                        COUNT(DISTINCT CountLocIn) AS LocsIn
                    FROM
                    locsProdF
                    GROUP BY Fecha , Skill , Grupo";
            $this->db->query("CREATE TEMPORARY TABLE kpisVenta $query"); 
            // $q = $this->db->query($query);
            // $q = $this->db->query("SELECT * FROM locsProdF");
            // okResponse('data', 'test', $q->result_array(),$this, 'q', $query);
        // =================================================
        // END Query KPIs Venta
        // =================================================

        // =================================================
        // START Query Calls
        // =================================================
            $this->db->query("DROP TEMPORARY TABLE IF EXISTS calls");
            $this->db->query("CREATE TEMPORARY TABLE calls SELECT 
                                                Fecha,
                                                Skill,
                                                CASE
                                                    WHEN grupo = 'pdv' THEN 'PDV'
                                                    WHEN grupo = 'Apoyo' THEN 'Apoyo'
                                                    ELSE CASE
                                                        WHEN dep = 0 THEN 'Otros'
                                                        WHEN
                                                            Skill = 5
                                                        THEN
                                                            CASE
                                                                WHEN dep = 5 THEN 'CC'
                                                                ELSE 'Otros'
                                                            END
                                                        ELSE 'CC'
                                                    END
                                                END AS grupoOK,
                                                SUM(IF(direction = 1, calls, 0)) AS inOfrecidas,
                                                SUM(IF(direction = 1 AND grupo != 'abandon' AND grupo != 'desborde',
                                                    calls,
                                                    0)) AS inContestadas,
                                                SUM(IF(direction = 1 AND grupo = 'desborde',
                                                    calls,
                                                    0)) AS inDesbordadas,
                                                SUM(IF(direction = 1 AND grupo = 'abandon',
                                                    calls,
                                                    0)) AS inAbandonadas,
                                                SUM(IF(direction = 1 AND grupo != 'abandon',
                                                    IF(isVenta,sla20,sla30),
                                                    0)) AS inSLA,
                                                SUM(IF(direction = 1 AND grupo != 'abandon',
                                                    TT,
                                                    0)) AS inTT,
                                                SUM(IF(direction = 1,
                                                    waitT,
                                                    0)) AS inWait,
                                                SUM(IF(direction = 1 AND grupo != 'abandon',
                                                    xfered,
                                                    0)) AS inXfered,
                                                SUM(IF(direction = 2 AND grupo != 'abandon',
                                                    outEfectivas,
                                                    0)) AS outEfectivas,
                                                SUM(IF(direction = 2 AND grupo != 'abandon',
                                                    outEfectivasTT,
                                                    0)) AS outEfectivasTT,
                                                SUM(IF(direction = 2,
                                                    outIntentos,
                                                    0)) AS outIntentos,
                                                SUM(IF(direction = 2,
                                                    outIntentosTT,
                                                    0)) AS outIntentosTT
                                            FROM
                                                calls_summary a
                                                    LEFT JOIN
                                                PCRCs pr ON a.Skill = pr.id
                                            WHERE
                                                Fecha BETWEEN @inicio AND @fin
                                                    AND a.Skill IN ".$params['skill']."
                                            GROUP BY Fecha, Skill, grupoOK");
        // =================================================
        // END Query Calls
        // =================================================

        // =================================================
        // START Query Logueo Asesores
        // =================================================
            $this->db->query("DROP TEMPORARY TABLE IF EXISTS logAsesores");
            $this->db->query("CREATE TEMPORARY TABLE logAsesores SELECT 
                                a.*, IF(skill = dep, a.asesor, NULL) AS DistAsesor, dep, cc
                            FROM
                                asesores_logs a
                                    LEFT JOIN
                                dep_asesores b ON a.asesor = b.asesor
                                    AND CAST(login AS DATE) = b.Fecha
                                    LEFT JOIN
                                cc_apoyo d ON a.asesor = d.asesor
                                    AND CAST(login AS DATE) BETWEEN d.inicio AND d.fin
                            WHERE
                                login BETWEEN @inicio AND CAST(CONCAT(@fin, ' 23:59:00') AS DATETIME)
                                    AND skill IN ".$params['skill']);

            $this->db->query("DROP TEMPORARY TABLE IF EXISTS logsOK");
            $query = "SELECT 
                                CAST(login AS DATE) AS FechaOK,
                                skill,
                                CASE
                                    WHEN asesor=0 THEN 'Otros'
                                    WHEN dep IN (29,56) AND cc IS NULL THEN 'PDV'
                                    WHEN dep IN (29,56) AND cc IS NOT NULL THEN 'Apoyo'
                                    WHEN skill = 5 THEN 
                                    CASE 
                                        WHEN dep=5 THEN 'CC'
                                        ELSE 'Otros'
                                    END
                                    ELSE 'CC'
                                END as grupo,
                                SUM(TIME_TO_SEC(Duracion)) AS Sesion,
                                SUM(TIME_TO_SEC(Duracion))/28800 AS Ftes,
                                COUNT(DISTINCT DistAsesor) as HC_dia
                            FROM
                                logAsesores
                            GROUP BY FechaOK , skill, grupo";
            $this->db->query("CREATE TEMPORARY TABLE logsOK $query");
            
        // =================================================
        // END Query Logueo Asesores
        // =================================================

        // =================================================
        // START Query Pausas
        // =================================================
            $this->db->query("DROP TEMPORARY TABLE IF EXISTS pauseAsesores");
            $this->db->query("CREATE TEMPORARY TABLE pauseAsesores SELECT 
                                    a.*, Productiva, IF(l.skill = dep, a.asesor, NULL) AS DistAsesor, dep, cc
                                FROM
                                    asesores_logs l LEFT JOIN
                                    asesores_pausas a ON a.asesor = l.asesor
                                        AND CAST(a.Inicio AS DATE) = CAST(login AS DATE)
                                        LEFT JOIN
                                    dep_asesores b ON a.asesor = b.asesor
                                        AND CAST(a.Inicio AS DATE) = b.Fecha LEFT JOIN Tipos_pausas c ON c.pausa_id = a.tipo 
                                        LEFT JOIN
                                    cc_apoyo d ON a.asesor = d.asesor
                                        AND CAST(a.Inicio AS DATE) BETWEEN d.inicio AND d.fin
                                WHERE
                                    login BETWEEN @inicio AND CAST(CONCAT(CAST(@fin as DATE), ' 23:59:00') AS DATETIME)
                                        AND l.skill IN ".$params['skill']);

                                $this->db->query("DROP TEMPORARY TABLE IF EXISTS pauseOK");
                                $this->db->query("CREATE TEMPORARY TABLE pauseOK SELECT 
                                    CAST(a.Inicio AS DATE) AS FechaOK,
                                    skill,
                                    CASE
                                        WHEN asesor=0 THEN 'Otros'
                                        WHEN dep IN (29,56) AND cc IS NULL THEN 'PDV'
                                        WHEN dep IN (29,56) AND cc IS NOT NULL THEN 'Apoyo'
                                        WHEN skill = 5 THEN 
                                        CASE 
                                            WHEN dep=5 THEN 'CC'
                                            ELSE 'Otros'
                                        END
                                        ELSE 'CC'
                                    END as grupo,
                                    SUM(IF(Productiva = 0,
                                        TIME_TO_SEC(Duracion),
                                        0)) AS PNP,
                                    SUM(IF(Productiva = 1,
                                        TIME_TO_SEC(Duracion),
                                        0)) AS PP
                                FROM
                                    pauseAsesores a
                                GROUP BY FechaOK , skill, grupo HAVING  FechaOK IS NOT NULL");
                                
        // =================================================
        // END Query Pausas
        // =================================================
        
        // =================================================
        // START Query Consolidación Log y Pausa
        // =================================================
            $this->db->query("DROP TEMPORARY TABLE IF EXISTS sesionesOK");
            $this->db->query("CREATE TEMPORARY TABLE sesionesOK SELECT 
                a.*,
                COALESCE(PNP,0) as PNP,
                COALESCE(PP,0) as PP,
                Sesion - COALESCE(PNP,0) AS Utilizacion
            FROM
                logsOK a
                    LEFT JOIN
                pauseOK b ON a.FechaOK = b.FechaOK
                    AND a.skill = b.skill AND a.grupo=b.grupo");
        // =================================================
        // END Query Consolidación Log y Pausa
        // =================================================

        // =================================================
        // START Query KPIs Telefónicos
        // =================================================
            $this->db->query("DROP TEMPORARY TABLE IF EXISTS kpisTel");
            $this->db->query("CREATE TEMPORARY TABLE kpisTel
                    SELECT 
                        a.*,
                        Sesion, PNP, PP, Utilizacion,
                        Ftes, HC_dia
                    FROM
                        calls a
                            LEFT JOIN
                        sesionesOK d ON a.Fecha = d.FechaOK
                            AND a.skill = d.skill AND a.grupoOK=d.grupo
                    GROUP BY a.Fecha , a.Skill, grupoOK;");
        // =================================================
        // END Query KPIs Telefónicos
        // =================================================
        
        // =================================================
        // START Query Resultados
        // =================================================
            $this->db->query("DROP TEMPORARY TABLE IF EXISTS result");
            $this->db->query("CREATE TEMPORARY TABLE result
                                SELECT 
                                    b.Fecha, b.Skill, b.grupoOK as grupo, IF(b.Skill=0, 'Online', NOMBREDEP(b.Skill)) as Dep,
                                    COALESCE(monto,0) as monto, 
                                    COALESCE(monto_hotel,0) as monto_hotel, 
                                    COALESCE(monto_tour,0) as monto_tour, 
                                    COALESCE(monto_transfer,0) as monto_transfer, 
                                    COALESCE(monto,0)-COALESCE(margen,0) as margen,
                                    COALESCE(monto_hotel,0)-COALESCE(margen_hotel,0) as margen_hotel,
                                    COALESCE(RN,0) as RN, 
                                    COALESCE(LocsIn,0) as LocsIn, 
                                    COALESCE(LocsOut,0) as LocsOut, 
                                    COALESCE(inOfrecidas,0) as inOfrecidas,
                                    COALESCE(inAbandonadas,0) as inAbandonadas,
                                    COALESCE(inDesbordadas,0)/COALESCE(inOfrecidas,0) as pDesborde,
                                    COALESCE(inAbandonadas,0)/COALESCE(inOfrecidas,0) as pAbandon,
                                    COALESCE(inSLA,0) as inSLA, 
                                    IF(COALESCE(inOfrecidas,0) = 0, 0, COALESCE(inSLA,0)/COALESCE(inOfrecidas,0)) as pSLA, 
                                    COALESCE(inWait,0) as inWait,
                                    COALESCE(inContestadas,0) as inContestadas,
                                    COALESCE(inDesbordadas,0) as inDesbordadas,
                                    COALESCE(inTT,0) as inTT, 
                                    COALESCE(inTT,0)/COALESCE(inContestadas,0) as inAHT, 
                                    COALESCE(inXfered,0) as inXfered,
                                    IF(COALESCE(inContestadas,0)-COALESCE(inXfered,0) = 0, 0, COALESCE(LocsIn,0)/(COALESCE(inContestadas,0)-COALESCE(inXfered,0))) as FC, 
                                    COALESCE(outEfectivas,0) as outEfectivas, 
                                    COALESCE(outEfectivasTT,0) as outEfectivasTT,
                                    COALESCE(outEfectivasTT,0)/ COALESCE(outEfectivas,0) as outEfectivasAHT,
                                    COALESCE(outIntentos,0) as outIntentos, 
                                    COALESCE(outIntentosTT,0) as outIntentosTT, 
                                    COALESCE(Sesion,0) as Sesion, 
                                    COALESCE(PNP,0) as PNP, 
                                    COALESCE(PP,0) as PP,
                                    COALESCE(Utilizacion,0) as Utilizacion, 
                                    COALESCE(Utilizacion,0)/COALESCE(Sesion,0) as pUtilizacion, 
                                    COALESCE(inTT,0) + COALESCE(outEfectivasTT,0) + COALESCE(outIntentosTT,0) + COALESCE(PP,0) as Ocupacion, 
                                    (COALESCE(inTT,0) + COALESCE(outEfectivasTT,0) + COALESCE(outIntentosTT,0) + COALESCE(PP,0))/COALESCE(Utilizacion,0) as pOcupacion, 
                                    COALESCE(Ftes,0) as Ftes, 
                                    COALESCE(HC_dia,0) as HC_dia
                                FROM
                                    kpisVenta a
                                        RIGHT JOIN
                                    kpisTel b ON a.Fecha = b.Fecha AND a.Skill = b.Skill AND a.grupo=b.grupoOK");
        // =================================================
        // END Query Resultados
        // =================================================

        // =================================================
        // START BUILD JSON FOR API
        // =================================================

            if( $q = $this->db->query("SELECT * FROM result") ){
                
                // =================================================
                // START BASE QUERY
                // =================================================
                    $query = "SELECT
                                Fecha, Skill, grupo, IF(Skill=0, 'Online', NOMBREDEP(Skill)) as Dep,
                                SUM(monto) as monto, 
                                SUM(monto_hotel) as monto_hotel, 
                                SUM(monto_tour) as monto_tour, 
                                SUM(monto_transfer) as monto_transfer, 
                                SUM(margen) as margen, 
                                SUM(margen_hotel) as margen_hotel, 
                                SUM(RN) as RN, 
                                SUM(LocsIn) as LocsIn, 
                                SUM(LocsOut) as LocsOut, 
                                SUM(inOfrecidas) as inOfrecidas, 
                                SUM(inAbandonadas) as inAbandonadas, 
                                SUM(inAbandonadas)/SUM(inOfrecidas) as pAbandon, 
                                SUM(inDesbordadas)/SUM(inOfrecidas) as pDesborde, 
                                SUM(inSLA) as inSLA, 
                                IF(SUM(inOfrecidas) = 0,0,SUM(inSLA)/SUM(inOfrecidas)) as pSLA, 
                                SUM(inWait) as inWait, 
                                SUM(inContestadas) as inContestadas, 
                                SUM(inDesbordadas) as inDesbordadas, 
                                SUM(inTT) as inTT, 
                                SUM(inTT)/SUM(inContestadas) as inAHT, 
                                SUM(inXfered) as inXfered, 
                                SUM(LocsIn)/SUM(inContestadas) as FC, 
                                SUM(outEfectivas) as outEfectivas, 
                                SUM(outEfectivasTT) as outEfectivasTT, 
                                SUM(outEfectivasTT)/SUM(outEfectivas) as outEfectivasAHT, 
                                SUM(outIntentos) as outIntentos, 
                                SUM(outIntentosTT) as outIntentosTT, 
                                SUM(Sesion) as Sesion, 
                                SUM(PNP) as PNP, 
                                SUM(PP) as PP, 
                                SUM(Utilizacion) as Utilizacion, 
                                SUM(Utilizacion)/SUM(Sesion) as pUtilizacion, 
                                SUM(Ocupacion) as Ocupacion, 
                                SUM(Ocupacion)/SUM(Utilizacion) as pOcupacion, 
                                SUM(Ftes) as Ftes, 
                                SUM(HC_dia) as HC_dia
                            FROM
                            result GROUP BY";
                // =================================================
                // END BASE QUERY
                // =================================================
                

                $td = $this->db->query("$query Fecha, Skill");
                $tg = $this->db->query(str_replace('SUM(Ftes)','SUM(Ftes)/COUNT(DISTINCT Fecha)',str_replace('SUM(HC_dia)','SUM(HC_dia)/COUNT(DISTINCT Fecha)',$query))." Skill, grupo");
                $ts = $this->db->query(str_replace('SUM(Ftes)','SUM(Ftes)/COUNT(DISTINCT Fecha)',str_replace('SUM(HC_dia)','SUM(HC_dia)/COUNT(DISTINCT Fecha)',$query))." Skill");
                    
                    foreach( $q->result_array() as $index => $info ){
                        $result[$info['Dep']][$info['grupo']][$info['Fecha']] = $info;
                    }
                    
                    foreach( $td->result_array() as $index => $info ){
                        $result[$info['Dep']]['all'][$info['Fecha']] = $info;
                    }
                
                    foreach( $tg->result_array() as $index => $info ){
                        $result[$info['Dep']][$info['grupo']]['Total'] = $info;
                    }
                
                    foreach( $ts->result_array() as $index => $info ){
                        $result[$info['Dep']]['all']['Total'] = $info;
                    }
                    
                    okResponse('Información Obtenida', 'data', $result, $this);
                
            }else{
            errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }
        // =================================================
        // END BUILD JSON FOR API
        // =================================================

        return true;
    });
      
  }

}
