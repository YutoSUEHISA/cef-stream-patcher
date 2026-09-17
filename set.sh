make clean
autoconf
automake
./configure --enable-debug
make
make install
ldconfig