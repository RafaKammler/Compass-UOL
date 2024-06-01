def pares_ate(n:int):
    for i in range(2, n+1):
        if i % 2 == 0:
            yield i
        else:
            pass
        
pares_ate_20 = pares_ate(20)
for n in pares_ate_20:
    print(n)