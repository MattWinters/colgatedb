package tutorials.concurrency_tutorial;

/* From Java Tutorial (see copyright below):
http://docs.oracle.com/javase/tutorial/essential/concurrency/deadlock.html
 */

public class Deadlock {
    static class Friend {
        private final String name;
        public Friend(String name) {
            this.name = name;
        }
        public String getName() {
            return this.name;
        }
        public synchronized void bow(Friend bower) {
            System.out.format("%s: %s"
                + " has bowed to me!%n",
                this.name, bower.getName());
            bower.bowBack(this);
        }
        public synchronized void bowBack(Friend bower) {
            System.out.format("%s: %s"
                + " has bowed back to me!%n",
                this.name, bower.getName());
        }
    }

    public static void main(String[] args) {
        final Friend alphonse =
            new Friend("Alphonse");
        final Friend gaston =
            new Friend("Gaston");
        new Thread(new Runnable() {
            public void run() { alphonse.bow(gaston); }
        }).start();
        new Thread(new Runnable() {
            public void run() { gaston.bow(alphonse); }
        }).start();
    }
}

/** Copyright and License: The Java SE Tutorial

 Copyright 1995, 2014, Oracle Corporation and/or its affiliates (Oracle). All
rights reserved.

 This tutorial is a guide to developing applications for the Java Platform,
Standard Edition and contains documentation (Tutorial) and sample code. The
sample code made available with this Tutorial is licensed separately to you by
Oracle under the Berkeley license. If you download any such sample code, you
agree to the terms of the Berkeley license.

 This Tutorial is provided to you by Oracle under the following license terms
containing restrictions on use and disclosure and is protected by intellectual
property laws. Oracle grants to you a limited, non-exclusive license to use
this Tutorial for information purposes only, as an aid to learning about the
Java SE platform. Except as expressly permitted in these license terms, you
may not use, copy, reproduce, translate, broadcast, modify, license, transmit,
distribute, exhibit, perform, publish, or display any part, in any form, or by
any means this Tutorial. Reverse engineering, disassembly, or decompilation of
this Tutorial is prohibited.

 The information contained herein is subject to change without notice and is
not warranted to be error-free. If you find any errors, please report them to
us in writing.

 If the Tutorial is licensed on behalf of the U.S. Government, the following
notice is applicable:

 U.S. GOVERNMENT RIGHTS Programs, software, databases, and related
documentation and technical data delivered to U.S. Government customers are
"commercial computer software" or "commercial technical data" pursuant to the
applicable Federal Acquisition Regulation and agency-specific supplemental
regulations. As such, the use, duplication, disclosure, modification, and
adaptation shall be subject to the restrictions and license terms set forth in
the applicable Government contract, and, to the extent applicable by the terms
of the Government contract, the additional rights set forth in FAR 52.227-19,
Commercial Computer Software License (December 2007). Oracle USA, Inc., 500
Oracle Parkway, Redwood City, CA 94065.

 This Tutorial is not developed or intended for use in any inherently
dangerous applications, including applications which may create a risk of
personal injury. If you use this Tutorial in dangerous applications, then you
shall be responsible to take all appropriate fail-safe, backup, redundancy,
and other measures to ensure the safe use.

 THE TUTORIAL IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND. ORACLE FURTHER
DISCLAIMS ALL WARRANTIES, EXPRESS AND IMPLIED, INCLUDING WITHOUT LIMITATION,
ANY IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE OR
NONINFRINGEMENT.

 IN NO EVENT SHALL ORACLE BE LIABLE FOR ANY INDIRECT, INCIDENTAL, SPECIAL,
PUNITIVE OR CONSEQUENTIAL DAMAGES, OR DAMAGES FOR LOSS OF PROFITS, REVENUE,
DATA OR DATA USE, INCURRED BY YOU OR ANY THIRD PARTY, WHETHER IN AN ACTION IN
CONTRACT OR TORT, EVEN IF ORACLE HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH
DAMAGES. ORACLE'S ENTIRE LIABILITY FOR DAMAGES HEREUNDER SHALL IN NO EVENT
EXCEED ONE THOUSAND DOLLARS (U.S. $1,000).

 No Technical Support

 Oracle's technical support organization will not provide technical support,
phone support, or updates to you.

 Oracle and Java are registered trademarks of Oracle and/or its affiliates.
Other names may be trademarks of their respective owners.

 The sample code and Tutorial may provide access to or information on content,
products, and services from third parties. Oracle Corporation and its
affiliates are not responsible for and expressly disclaim all warranties of
any kind with respect to third-party content, products, and services. Oracle
Corporation and its affiliates will not be responsible for any loss, costs, or
damages incurred due to your access to or use of third-party content,
products, or services.
 */