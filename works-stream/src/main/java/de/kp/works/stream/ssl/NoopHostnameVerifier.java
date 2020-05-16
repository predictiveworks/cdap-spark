package de.kp.works.stream.ssl;
/*
 * Copyright (c) 2020 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 * 
 */

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;

/**
 * The NO_OP HostnameVerifier essentially turns hostname verification
 * off. This implementation is a no-op, and never throws the SSLException.
 */
public class NoopHostnameVerifier implements HostnameVerifier {

    public static final NoopHostnameVerifier INSTANCE = new NoopHostnameVerifier();

    @Override
    public boolean verify(final String s, final SSLSession sslSession) {
        return true;
    }

    @Override
    public final String toString() {
        return "NO_OP";
    }

}

