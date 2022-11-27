import pytest
@pytest.mark.parametrize("passd, strength",[('san','Medium'),('Passwords1','Strong')])
def test_passwords_params(passd, strength):
    if len(passd) < 4:
        num ='Medium'
    else:
        num ='Strong'
    assert num == strength