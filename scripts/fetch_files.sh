#!/bin/sh

cd /home/drews/boinc/data

cd ./seti
wget -N --tries=2 -nv http://setiweb.ssl.berkeley.edu/stats/host.gz
wget -N --tries=2 -nv http://setiweb.ssl.berkeley.edu/stats/team.gz
wget -N --tries=2 -nv http://setiweb.ssl.berkeley.edu/stats/user.gz

cd ../einstein
wget -N --tries=2 -nv http://einstein.phys.uwm.edu/stats/host_id.gz
wget -N --tries=2 -nv http://einstein.phys.uwm.edu/stats/team_id.gz
wget -N --tries=2 -nv http://einstein.phys.uwm.edu/stats/user_id.gz

cd ../predictor
wget -N --tries=2 -nv http://predictor.scripps.edu/stats/host_id.gz
wget -N --tries=2 -nv http://predictor.scripps.edu/stats/team_id.gz
wget -N --tries=2 -nv http://predictor.scripps.edu/stats/user_id.gz

cd ../climate
wget -N --tries=2 -nv http://climateapps2.oucs.ox.ac.uk/cpdnboinc/stats/host.xml.gz
wget -N --tries=2 -nv http://climateapps2.oucs.ox.ac.uk/cpdnboinc/stats/team.xml.gz
wget -N --tries=2 -nv http://climateapps2.oucs.ox.ac.uk/cpdnboinc/stats/user.xml.gz

cd ../lhc
wget -N --tries=2 -nv http://lhcathome.cern.ch/stats/host_id.gz
wget -N --tries=2 -nv http://lhcathome.cern.ch/stats/team_id.gz
wget -N --tries=2 -nv http://lhcathome.cern.ch/stats/user_id.gz

cd ../rosetta
wget -N --tries=2 -nv http://boinc.bakerlab.org/rosetta/stats/host.gz
wget -N --tries=2 -nv http://boinc.bakerlab.org/rosetta/stats/team.gz
wget -N --tries=2 -nv http://boinc.bakerlab.org/rosetta/stats/user.gz

cd ../burp
wget -N --tries=2 -nv http://burp.boinc.dk/stats/host_id.gz
wget -N --tries=2 -nv http://burp.boinc.dk/stats/team_id.gz
wget -N --tries=2 -nv http://burp.boinc.dk/stats/user_id.gz

cd ../primegrid
wget -N --tries=2 -nv http://www.primegrid.com/stats/host_id.gz
wget -N --tries=2 -nv http://www.primegrid.com/stats/team_id.gz
wget -N --tries=2 -nv http://www.primegrid.com/stats/user_id.gz

cd ../qmc
wget -N --tries=2 -nv http://qah.uni-muenster.de/stats/host.gz
wget -N --tries=2 -nv http://qah.uni-muenster.de/stats/team.gz
wget -N --tries=2 -nv http://qah.uni-muenster.de/stats/user.gz

cd ../sztaki
wget -N --tries=2 -nv http://szdg.lpds.sztaki.hu/szdg/stats/host.xml.gz 
wget -N --tries=2 -nv http://szdg.lpds.sztaki.hu/szdg/stats/team.xml.gz 
wget -N --tries=2 -nv http://szdg.lpds.sztaki.hu/szdg/stats/user.xml.gz 

#cd ../folding
#wget -N --tries=2 -nv http://fah-boinc.stanford.edu/stats/host.gz
#wget -N --tries=2 -nv http://fah-boinc.stanford.edu/stats/team.gz
#wget -N --tries=2 -nv http://fah-boinc.stanford.edu/stats/user.gz

cd ../wcg
wget -N --tries=2 -nv http://www.worldcommunitygrid.org/boinc/stats/host.gz
wget -N --tries=2 -nv http://www.worldcommunitygrid.org/boinc/stats/team.gz
wget -N --tries=2 -nv http://www.worldcommunitygrid.org/boinc/stats/user.gz

cd ../ufluids
wget -N --tries=2 -nv http://www.ufluids.net/stats/host.xml.gz
wget -N --tries=2 -nv http://www.ufluids.net/stats/team.xml.gz
wget -N --tries=2 -nv http://www.ufluids.net/stats/user.xml.gz

cd ../mcp
wget -N --tries=2 -nv http://www.malariacontrol.net/stats/host.gz
wget -N --tries=2 -nv http://www.malariacontrol.net/stats/team.gz
wget -N --tries=2 -nv http://www.malariacontrol.net/stats/user.gz

cd ../lattice2
wget -N --tries=2 -nv http://boinc.umiacs.umd.edu/stats/host.gz
wget -N --tries=2 -nv http://boinc.umiacs.umd.edu/stats/team.gz
wget -N --tries=2 -nv http://boinc.umiacs.umd.edu/stats/user.gz

cd ../simap
wget -N --tries=2 -nv http://boinc.bio.wzw.tum.de/boincsimap/stats/host.gz
wget -N --tries=2 -nv http://boinc.bio.wzw.tum.de/boincsimap/stats/team.gz
wget -N --tries=2 -nv http://boinc.bio.wzw.tum.de/boincsimap/stats/user.gz

cd ../pirates
wget -N --tries=2 -nv http://pirates.spy-hill.net/stats/host_id.gz
wget -N --tries=2 -nv http://pirates.spy-hill.net/stats/team_id.gz
wget -N --tries=2 -nv http://pirates.spy-hill.net/stats/user_id.gz

cd ../seti-beta
wget -N --tries=2 -nv http://setiweb.ssl.berkeley.edu/beta/stats/host.gz
wget -N --tries=2 -nv http://setiweb.ssl.berkeley.edu/beta/stats/team.gz
wget -N --tries=2 -nv http://setiweb.ssl.berkeley.edu/beta/stats/user.gz

cd ../bbc-cpdn
wget -N --tries=2 -nv http://bbc.cpdn.org/stats/host.xml.gz
wget -N --tries=2 -nv http://bbc.cpdn.org/stats/team.xml.gz
wget -N --tries=2 -nv http://bbc.cpdn.org/stats/user.xml.gz

cd ../leiden
wget -N --tries=2 -nv http://boinc.gorlaeus.net/stats/host.xml.gz
wget -N --tries=2 -nv http://boinc.gorlaeus.net/stats/team.xml.gz
wget -N --tries=2 -nv http://boinc.gorlaeus.net/stats/user.xml.gz

cd ../ralph
wget -N --tries=2 -nv http://ralph.bakerlab.org/stats/host.gz
wget -N --tries=2 -nv http://ralph.bakerlab.org/stats/team.gz
wget -N --tries=2 -nv http://ralph.bakerlab.org/stats/user.gz

cd ../xtremlab
wget -N --tries=2 -nv http://xw01.lri.fr:4320/stats/host.gz
wget -N --tries=2 -nv http://xw01.lri.fr:4320/stats/team.gz
wget -N --tries=2 -nv http://xw01.lri.fr:4320/stats/user.gz

cd ../hc
wget -N --tries=2 -nv http://boinc.banaan.org/hashclash/stats/host.gz
wget -N --tries=2 -nv http://boinc.banaan.org/hashclash/stats/team.gz
wget -N --tries=2 -nv http://boinc.banaan.org/hashclash/stats/user.gz

cd ../cpdns
wget -N --tries=2 -nv http://attribution.cpdn.org/stats/host.xml.gz
wget -N --tries=2 -nv http://attribution.cpdn.org/stats/team.xml.gz
wget -N --tries=2 -nv http://attribution.cpdn.org/stats/user.xml.gz

cd ../chess
wget -N --tries=2 -nv http://www.chess960athome.org/alpha/stats/host.gz
wget -N --tries=2 -nv http://www.chess960athome.org/alpha/stats/user.gz
wget -N --tries=2 -nv http://www.chess960athome.org/alpha/stats/team.gz

cd ../vtu
wget -N --tries=2 -nv http://boinc.vtu.lt/vtuathome/stats/host.gz
wget -N --tries=2 -nv http://boinc.vtu.lt/vtuathome/stats/user.gz
wget -N --tries=2 -nv http://boinc.vtu.lt/vtuathome/stats/team.gz

cd ../lhcalpha
wget -N --tries=2 -nv http://lhcathome-alpha.cern.ch/stats/host_id.gz
wget -N --tries=2 -nv http://lhcathome-alpha.cern.ch/stats/user_id.gz
wget -N --tries=2 -nv http://lhcathome-alpha.cern.ch/stats/team_id.gz

cd ../tanpaku
wget -N --tries=2 -nv http://issofty17.is.noda.tus.ac.jp/stats/host.gz
wget -N --tries=2 -nv http://issofty17.is.noda.tus.ac.jp/stats/user.gz
wget -N --tries=2 -nv http://issofty17.is.noda.tus.ac.jp/stats/team.gz

cd ../rcn
wget -N --tries=2 -nv http://dist.ist.tugraz.at/cape5/stats/host.gz
wget -N --tries=2 -nv http://dist.ist.tugraz.at/cape5/stats/user.gz
wget -N --tries=2 -nv http://dist.ist.tugraz.at/cape5/stats/team.gz

cd ../nh
wget -N --tries=2 -nv http://www.nanohive-1.org/atHome/stats/host.gz
wget -N --tries=2 -nv http://www.nanohive-1.org/atHome/stats/user.gz
wget -N --tries=2 -nv http://www.nanohive-1.org/atHome/stats/team.gz

cd ../sh
wget -N --tries=2 -nv http://spin.fh-bielefeld.de/stats/host.gz
wget -N --tries=2 -nv http://spin.fh-bielefeld.de/stats/user.gz
wget -N --tries=2 -nv http://spin.fh-bielefeld.de/stats/team.gz

cd ../rs
wget -N --tries=2 -nv http://boinc.rieselsieve.com/stats/host_id.gz
wget -N --tries=2 -nv http://boinc.rieselsieve.com/stats/user_id.gz
wget -N --tries=2 -nv http://boinc.rieselsieve.com/stats/team_id.gz

cd ../neuron
wget -N --tries=2 -nv http://neuron.mine.nu/neuron/stats/host.gz
wget -N --tries=2 -nv http://neuron.mine.nu/neuron/stats/user.gz
wget -N --tries=2 -nv http://neuron.mine.nu/neuron/stats/team.gz

cd ../rah
wget -N --tries=2 -nv http://www.renderfarmathome.com.ar/stats/host.gz
wget -N --tries=2 -nv http://www.renderfarmathome.com.ar/stats/user.gz
wget -N --tries=2 -nv http://www.renderfarmathome.com.ar/stats/team.gz

#cd ../nsh
#wget -N --tries=2 -nv http://slawoo.homelinux.org/stats/host.gz
#wget -N --tries=2 -nv http://slawoo.homelinux.org/stats/user.gz
#wget -N --tries=2 -nv http://slawoo.homelinux.org/stats/team.gz

cd ../docking
wget -N --tries=2 -nv http://docking.utep.edu/stats/host.gz
wget -N --tries=2 -nv http://docking.utep.edu/stats/user.gz
wget -N --tries=2 -nv http://docking.utep.edu/stats/team.gz

cd ../proteins
wget -N --tries=2 -nv http://bioc.polytechnique.fr/proteinsathome/stats/host.gz
wget -N --tries=2 -nv http://bioc.polytechnique.fr/proteinsathome/stats/user.gz
wget -N --tries=2 -nv http://bioc.polytechnique.fr/proteinsathome/stats/team.gz

cd ../depspid
wget -N --tries=2 -nv http://www.depspid.net/stats/host_id.gz
wget -N --tries=2 -nv http://www.depspid.net/stats/user_id.gz
wget -N --tries=2 -nv http://www.depspid.net/stats/team_id.gz

cd ../abc
wget -N --tries=2 -nv http://abcathome.com/stats/host.gz
wget -N --tries=2 -nv http://abcathome.com/stats/user.gz
wget -N --tries=2 -nv http://abcathome.com/stats/team.gz

cd ../alpha
wget -N --tries=2 -nv http://isaac.ssl.berkeley.edu/alpha/stats/host.gz
wget -N --tries=2 -nv http://isaac.ssl.berkeley.edu/alpha/stats/user.gz
wget -N --tries=2 -nv http://isaac.ssl.berkeley.edu/alpha/stats/team.gz

cd ../abcbeta
wget -N --tries=2 -nv http://abcbeta.math.leidenuniv.nl/stats/host.gz
wget -N --tries=2 -nv http://abcbeta.math.leidenuniv.nl/stats/user.gz
wget -N --tries=2 -nv http://abcbeta.math.leidenuniv.nl/stats/team.gz

cd ../drtg
wget -N --tries=2 -nv http://hashbreaker.com:8700/tmrldrtg/stats/host.gz
wget -N --tries=2 -nv http://hashbreaker.com:8700/tmrldrtg/stats/user.gz
wget -N --tries=2 -nv http://hashbreaker.com:8700/tmrldrtg/stats/team.gz

cd ../wanless2
wget -N --tries=2 -nv http://bearnol.is-a-geek.com/wanless2/stats/host.gz
wget -N --tries=2 -nv http://bearnol.is-a-geek.com/wanless2/stats/user.gz
wget -N --tries=2 -nv http://bearnol.is-a-geek.com/wanless2/stats/team.gz

cd ../bebeer
wget -N --tries=2 -nv http://bebeer.dyndns.org:2222/bebeer/stats/host.gz
wget -N --tries=2 -nv http://bebeer.dyndns.org:2222/bebeer/stats/user.gz
wget -N --tries=2 -nv http://bebeer.dyndns.org:2222/bebeer/stats/team.gz

cd ../gerasim
wget -N --tries=2 -nv http://www.gerasim.boinc.ru/Gerasim/stats/host.xml.gz
wget -N --tries=2 -nv http://www.gerasim.boinc.ru/Gerasim/stats/user.xml.gz
wget -N --tries=2 -nv http://www.gerasim.boinc.ru/Gerasim/stats/team.xml.gz

cd ../zivis
wget -N --tries=2 -nv http://zivis.bifi.unizar.es/stats/host.gz
wget -N --tries=2 -nv http://zivis.bifi.unizar.es/stats/user.gz
wget -N --tries=2 -nv http://zivis.bifi.unizar.es/stats/team.gz

cd ../..
