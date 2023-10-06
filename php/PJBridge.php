<?php

namespace SmileyLettuce\JDBCBridge;

use Exception;
use Throwable;
use JsonException;

/*
 * Copyright (C) 2007 lenny@mondogrigio.cjb.net
 *
 * This file is part of PJBS (http://sourceforge.net/projects/pjbs)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

class PJBridge
{
    /** @var resource $sock */
    private $sock;

    /**
     * @param string $host
     * @param int $port
     * @throws Exception
     */
    public function __construct(string $host="localhost", int $port=4444)
    {

        $sock = fsockopen($host, $port, $error_code, $error_message);

        if (!$sock) {
            throw new Exception($error_code . ": " . $error_message);
        }

        $this->sock = $sock;
    }


    /**
     *
     */
    public function __destruct()
    {
        fclose($this->sock);
    }


    /**
     * @return array{'status': string, 'message': string ,"data"?: array<int, array<mixed>>}
     * @throws Exception
     */
    private function parse_reply(): array
    {
        /** @var array{'status': string, 'message': string ,"data"?: array<int, array<mixed>>} $ret */
        $ret = [];

        $returnString = fgets($this->sock);

        if($returnString === false) {
            throw new Exception('Unable to get ouput from socket');
        }

        try {
            $returnArr = json_decode($returnString, true, flags: JSON_BIGINT_AS_STRING | JSON_OBJECT_AS_ARRAY | JSON_THROW_ON_ERROR);
        } catch (JsonException $exception) {
            throw new Exception('Unable to decode contents');
        }

        if(is_array($returnArr) === false) {
            throw new Exception('Returned message not an array');
        }

        if(!isset($returnArr['status'])) {
            throw new Exception('Unable to get status');
        } else {
            $ret['status'] = $returnArr['status'];
        }

        if(array_key_exists('status', $returnArr) && $returnArr['status'] === "error") {
            throw new Exception($returnArr['message']);
        }

        if(array_key_exists('message', $returnArr)) {
            $ret['message'] = $returnArr['message'];
        }

        if(array_key_exists('data', $returnArr)) {
            $ret['data'] = $returnArr['data'];
        }

        return $ret;
    }


    /**
     * @param string $dsn
     * @param string $user
     * @param string $pass
     * @throws Exception
     */
    public function connect($dsn, $user, $pass): void
    {

        $connArr = [
            'action' => 'connect',
            'data' => [
                'user' => $user,
                'pass' => $pass,
                'dsn' => $dsn
            ]
        ];
        fwrite($this->sock, json_encode($connArr)."\n");
        $this->parse_reply();
    }


    /**
     * @param string $query
     * @param array<string,string> $params
     * @return string
     * @throws Exception
     */
    public function exec(string $query, array $params = []): string
    {

        $sendArr = [
            'action' => 'execute',
            'data'  => [
                'query' => $query
            ]
        ];

        if(!empty($params)) {

            foreach($params as $i=>$p) {
                $sendArr['data']['params'][$i] = $p;
            }
        }

        fwrite($this->sock, json_encode($sendArr)."\n");

        /**
         * @var array{status: string, message:string} $return
         */
        $return = $this->parse_reply();

        if(empty($return['message'])) {
            throw new Exception('ResultId is empty');
        }

        //this will return the result_id
        return $return['message'];
    }


    /**
     * @param string $resultId
     * @return mixed
     * @throws Exception
     */
    public function fetch(string $resultId): mixed
    {

        $sendArr = [
            'action' => 'fetch',
            'data'  => [
                'result_id' => $resultId
            ]
        ];

        fwrite($this->sock, json_encode($sendArr)."\n");

        /** @var array{'status': string, 'message': string ,'data': array<int, array<mixed>>} $return */
        $return = $this->parse_reply();

        if(!array_key_exists('data', $return)) {
            throw new Exception('data key is missing from the returned result');
        }

        /** @var mixed $dataRow */
        $dataRow = $return['data'][0];

        //this will return the result set
        return $dataRow;
    }


    /**
     * @param string $resultId
     * @return array<mixed>
     * @throws Exception
     */
    public function fetch_array(string $resultId): array
    {

        $sendArr = [
            'action' => 'fetch_array',
            'data'  => [
                'result_id' => $resultId
            ]
        ];

        fwrite($this->sock, json_encode($sendArr)."\n");

        /** @var array{'status': string, 'message': string ,'data': array<int, array<mixed>>} $return */
        $return = $this->parse_reply();

        if(!array_key_exists('data', $return)) {
            throw new Exception('data key is missing from the returned result');
        }

        /** @var array<mixed> $dataArray */
        $dataArray = $return['data'];

        //this will return the result set
        return $dataArray;
    }


    /**
     * @param string $resultId
     * @return string
     * @throws Exception
     */
    public function free_result(string $resultId): string
    {

        $sendArr = [
            'action' => 'free_result',
            'data'  => [
                'result_id' => $resultId
            ]
        ];

        fwrite($this->sock, json_encode($sendArr)."\n");

        /**
         * @var array{status: string, message:string} $return
         */
        $return = $this->parse_reply();

        if(empty($return['message'])) {
            throw new Exception('message is empty');
        }

        //this will return the result_id
        return $return['message'];
    }
}
