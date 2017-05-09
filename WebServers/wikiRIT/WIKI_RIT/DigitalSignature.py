
from fractions import gcd
import random

def DigitalSignatre(p,q):

    n = p*q

    phiOfn = n-1

    efound = False

    while efound!= True:

        e = random.randint(0, phiOfn)

        if gcd(e,phiOfn) == 1:

            efound = True


    d = e^(-1) % phiOfn


    





