/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* This file has been modified from its original version, which is
* under the Apache License:
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("AdbcDrivers.BigQuery.Tests, PublicKey=002400000480000094000000060200000024000052534131000400000100010079bc93ed967c25f57bf4d0457c18e090747c1f6af129f39e22e5595e893b68b4d6465c6e90edb13113f01966587ba861db598664b10cab7399c3dc40dc612f79d3bc1d430dbd5f1eda1587f85fefbaa3013ee5d3500d788a7855a6b46d9f1ff25daafa6377954280f8192e802080c0b92565808660c442c03210679fd2884eeb")]

// Castle DynamicProxy, which Moq generates its proxies into. Required for the
// test suite to mock internal types now that this assembly is strong-named.
[assembly: InternalsVisibleTo("DynamicProxyGenAssembly2, PublicKey=0024000004800000940000000602000000240000525341310004000001000100c547cac37abd99c8db225ef2f6c8a3602f3b3606cc9891605d02baa56104f4cfc0734aa39b93bf7852f7d9266654753cc297e7d2edfe0bac1cdcf9f717241550e0a7b191195b7667bb4f64bcb8e2121380fd1d9d46ad2d92d2d15605093924cceaf74c4861eff62abf69b9291ed0a340e113be11e6a7d3113e92484cf7045cc7")]
