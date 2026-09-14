
/opt/transparent-publisher-build/shoup-release-20260911/artifacts/transparent-shard-server:     file format elf64-x86-64


Disassembly of section .text:

000000000039e8b0 <valar_spiral_rs::poly::from_ntt>:
  39e8b0:	push   %rbp
  39e8b1:	push   %r15
  39e8b3:	push   %r14
  39e8b5:	push   %r13
  39e8b7:	push   %r12
  39e8b9:	push   %rbx
  39e8ba:	sub    $0x1b8,%rsp
  39e8c1:	mov    0x20(%rdi),%rax
  39e8c5:	mov    %rax,0x8(%rsp)
  39e8ca:	cmpb   $0x1,%fs:0xffffffffffffffb8
  39e8d3:	jne    39fc44 <valar_spiral_rs::poly::from_ntt+0x1394>
  39e8d9:	mov    %fs:0x0,%rax
  39e8e2:	lea    -0x70(%rax),%rbp
  39e8e9:	cmpq   $0x0,0x0(%rbp)
  39e8ee:	jne    39fc80 <valar_spiral_rs::poly::from_ntt+0x13d0>
  39e8f4:	movq   $0xffffffffffffffff,0x0(%rbp)
  39e8fc:	mov    0x28(%rdi),%rax
  39e900:	test   %rax,%rax
  39e903:	je     39fa71 <valar_spiral_rs::poly::from_ntt+0x11c1>
  39e909:	mov    0x30(%rdi),%r8
  39e90d:	test   %r8,%r8
  39e910:	je     39fa71 <valar_spiral_rs::poly::from_ntt+0x11c1>
  39e916:	mov    %rax,0xf0(%rsp)
  39e91e:	mov    0x18(%rbp),%rbx
  39e922:	mov    0x20(%rbp),%rcx
  39e926:	mov    0x8(%rsp),%rax
  39e92b:	add    $0x40,%rax
  39e92f:	mov    %rax,0x90(%rsp)
  39e937:	mov    0x10(%rsi),%rax
  39e93b:	mov    %rax,0x128(%rsp)
  39e943:	mov    0x20(%rsi),%rax
  39e947:	mov    %rax,0x130(%rsp)
  39e94f:	mov    0x30(%rsi),%rsi
  39e953:	mov    0x10(%rdi),%rax
  39e957:	mov    %rax,0x108(%rsp)
  39e95f:	lea    0x10(%rbx),%rax
  39e963:	mov    %rax,0x100(%rsp)
  39e96b:	lea    0x0(,%r8,8),%rax
  39e973:	mov    %rax,0xe8(%rsp)
  39e97b:	lea    0x1e8a6(%rip),%rax        # 3bd228 <tokio::runtime::task::waker::WAKER_VTABLE+0x1530>
  39e982:	mov    %rax,0x60(%rsp)
  39e987:	vpbroadcastq -0x35b770(%rip),%ymm0        # 43220 <GCC_except_table4170+0x213c>
  39e990:	xor    %eax,%eax
  39e992:	xor    %edi,%edi
  39e994:	mov    %rbp,0x18(%rsp)
  39e999:	mov    %rbx,0xb0(%rsp)
  39e9a1:	mov    %rcx,0x38(%rsp)
  39e9a6:	mov    %r8,0x118(%rsp)
  39e9ae:	mov    %rsi,0x110(%rsp)
  39e9b6:	vmovdqu %ymm0,0x160(%rsp)
  39e9bf:	mov    %rsi,%rdx
  39e9c2:	imul   %rdi,%rdx
  39e9c6:	mov    %rdx,0x138(%rsp)
  39e9ce:	inc    %rdi
  39e9d1:	mov    %rax,0xf8(%rsp)
  39e9d9:	mov    %rax,0xd8(%rsp)
  39e9e1:	xor    %r9d,%r9d
  39e9e4:	mov    %rdi,0x120(%rsp)
  39e9ec:	jmp    39ea25 <valar_spiral_rs::poly::from_ntt+0x175>
  39e9ee:	xchg   %ax,%ax
  39e9f0:	mov    0x140(%rsp),%r9
  39e9f8:	inc    %r9
  39e9fb:	addq   $0x8,0xd8(%rsp)
  39ea04:	mov    0x118(%rsp),%r8
  39ea0c:	cmp    %r8,%r9
  39ea0f:	mov    0x110(%rsp),%rsi
  39ea17:	mov    0x120(%rsp),%rdi
  39ea1f:	je     39fa4a <valar_spiral_rs::poly::from_ntt+0x119a>
  39ea25:	mov    0x130(%rsp),%rax
  39ea2d:	mov    0x40(%rax),%rdx
  39ea31:	imul   0x30(%rax),%rdx
  39ea36:	cmp    %rcx,%rdx
  39ea39:	ja     39fba6 <valar_spiral_rs::poly::from_ntt+0x12f6>
  39ea3f:	mov    0x8(%rsp),%rax
  39ea44:	mov    0x30(%rax),%rax
  39ea48:	mov    %rax,0x98(%rsp)
  39ea50:	mov    0x138(%rsp),%rax
  39ea58:	mov    %r9,0x140(%rsp)
  39ea60:	add    %r9,%rax
  39ea63:	imul   %rdx,%rax
  39ea67:	mov    0x128(%rsp),%rcx
  39ea6f:	lea    (%rcx,%rax,8),%rsi
  39ea73:	shl    $0x3,%rdx
  39ea77:	mov    %rbx,%rdi
  39ea7a:	vzeroupper
  39ea7d:	call   *0x1f045(%rip)        # 3bdac8 <memcpy@GLIBC_2.14>
  39ea83:	vpxor  %xmm11,%xmm11,%xmm11
  39ea88:	mov    0x90(%rsp),%rax
  39ea90:	mov    (%rax),%rax
  39ea93:	mov    %rax,0x10(%rsp)
  39ea98:	test   %rax,%rax
  39ea9b:	je     39f9d0 <valar_spiral_rs::poly::from_ntt+0x1120>
  39eaa1:	cmpq   $0x1,0x10(%rsp)
  39eaa7:	jne    39ee20 <valar_spiral_rs::poly::from_ntt+0x570>
  39eaad:	mov    0x8(%rsp),%rcx
  39eab2:	mov    0x30(%rcx),%r8
  39eab6:	mov    0x8(%rcx),%rax
  39eaba:	mov    0x10(%rcx),%rdi
  39eabe:	mov    0x38(%rcx),%rdx
  39eac2:	mov    %rdx,%rcx
  39eac5:	sub    $0x1,%rcx
  39eac9:	mov    $0x0,%esi
  39eace:	cmovae %rcx,%rsi
  39ead2:	test   %rdx,%rdx
  39ead5:	mov    %r8,0x10(%rsp)
  39eada:	je     39f4c8 <valar_spiral_rs::poly::from_ntt+0xc18>
  39eae0:	mov    %rdx,%r9
  39eae3:	mov    0x38(%rsp),%rdx
  39eae8:	cmp    %rdx,%r8
  39eaeb:	ja     39fc1f <valar_spiral_rs::poly::from_ntt+0x136f>
  39eaf1:	test   %rdi,%rdi
  39eaf4:	je     39fd76 <valar_spiral_rs::poly::from_ntt+0x14c6>
  39eafa:	mov    0x10(%rax),%rcx
  39eafe:	cmp    $0x3,%rcx
  39eb02:	jb     39fd88 <valar_spiral_rs::poly::from_ntt+0x14d8>
  39eb08:	je     39fd3a <valar_spiral_rs::poly::from_ntt+0x148a>
  39eb0e:	mov    0x8(%rax),%rax
  39eb12:	mov    0x38(%rax),%rcx
  39eb16:	mov    %rcx,0xa0(%rsp)
  39eb1e:	mov    0x40(%rax),%rdi
  39eb22:	mov    0x50(%rax),%rcx
  39eb26:	mov    %rcx,0x70(%rsp)
  39eb2b:	mov    0x58(%rax),%rax
  39eb2f:	mov    %rax,0x48(%rsp)
  39eb34:	mov    0x8(%rsp),%rax
  39eb39:	mov    0xa8(%rax),%rax
  39eb40:	mov    %rax,0x30(%rsp)
  39eb45:	add    %rax,%rax
  39eb48:	mov    %rax,0x28(%rsp)
  39eb4d:	mov    %r9,%rcx
  39eb50:	mov    %rdi,0x40(%rsp)
  39eb55:	jmp    39eb72 <valar_spiral_rs::poly::from_ntt+0x2c2>
  39eb57:	nopw   0x0(%rax,%rax,1)
  39eb60:	mov    0x68(%rsp),%rcx
  39eb65:	mov    %rcx,%rsi
  39eb68:	sub    $0x1,%rsi
  39eb6c:	jb     39f4ad <valar_spiral_rs::poly::from_ntt+0xbfd>
  39eb72:	mov    %rsi,%rax
  39eb75:	mov    0x10(%rsp),%rsi
  39eb7a:	shrx   %rcx,%rsi,%r9
  39eb7f:	mov    %r9,%r8
  39eb82:	add    %r9,%r8
  39eb85:	je     39fb55 <valar_spiral_rs::poly::from_ntt+0x12a5>
  39eb8b:	mov    %rax,0x68(%rsp)
  39eb90:	mov    %rsi,%rax
  39eb93:	or     %r8,%rax
  39eb96:	shr    $0x20,%rax
  39eb9a:	je     39ebb0 <valar_spiral_rs::poly::from_ntt+0x300>
  39eb9c:	mov    %rsi,%rax
  39eb9f:	xor    %edx,%edx
  39eba1:	div    %r8
  39eba4:	jmp    39ebb7 <valar_spiral_rs::poly::from_ntt+0x307>
  39eba6:	cs nopw 0x0(%rax,%rax,1)
  39ebb0:	mov    %esi,%eax
  39ebb2:	xor    %edx,%edx
  39ebb4:	div    %r8d
  39ebb7:	lea    -0x1(%rcx),%eax
  39ebba:	mov    $0x1,%ecx
  39ebbf:	shlx   %rax,%rcx,%r15
  39ebc4:	sub    %rdx,%rsi
  39ebc7:	mov    %rsi,0x50(%rsp)
  39ebcc:	test   %r9,%r9
  39ebcf:	je     39ede0 <valar_spiral_rs::poly::from_ntt+0x530>
  39ebd5:	mov    %r9,%rax
  39ebd8:	shl    $0x4,%rax
  39ebdc:	mov    %rax,0x78(%rsp)
  39ebe1:	lea    0x0(,%r9,8),%rax
  39ebe9:	mov    %rax,0xa8(%rsp)
  39ebf1:	mov    $0x1,%eax
  39ebf6:	mov    %rbx,0x20(%rsp)
  39ebfb:	xor    %edx,%edx
  39ebfd:	mov    %r8,0xc0(%rsp)
  39ec05:	mov    %r9,0xb8(%rsp)
  39ec0d:	mov    %r15,0x80(%rsp)
  39ec15:	data16 cs nopw 0x0(%rax,%rax,1)
  39ec20:	mov    %r15,%rcx
  39ec23:	mov    %rdx,%r15
  39ec26:	add    %rcx,%r15
  39ec29:	cmp    %rdi,%r15
  39ec2c:	jae    39fca9 <valar_spiral_rs::poly::from_ntt+0x13f9>
  39ec32:	cmp    0x48(%rsp),%r15
  39ec37:	jae    39fc98 <valar_spiral_rs::poly::from_ntt+0x13e8>
  39ec3d:	mov    %rax,0x88(%rsp)
  39ec45:	mov    0x50(%rsp),%rax
  39ec4a:	sub    %r8,%rax
  39ec4d:	jb     39fc8c <valar_spiral_rs::poly::from_ntt+0x13dc>
  39ec53:	mov    %rax,0x50(%rsp)
  39ec58:	mov    0xa0(%rsp),%rax
  39ec60:	mov    (%rax,%r15,8),%rax
  39ec64:	mov    %rax,0xd0(%rsp)
  39ec6c:	mov    0x70(%rsp),%rax
  39ec71:	mov    (%rax,%r15,8),%rax
  39ec75:	mov    %rax,0xc8(%rsp)
  39ec7d:	mov    0xa8(%rsp),%rax
  39ec85:	mov    0x20(%rsp),%rcx
  39ec8a:	add    %rcx,%rax
  39ec8d:	mov    %rax,0x58(%rsp)
  39ec92:	xor    %ebp,%ebp
  39ec94:	data16 data16 cs nopw 0x0(%rax,%rax,1)
  39eca0:	cmp    %rbp,%r8
  39eca3:	je     39fb26 <valar_spiral_rs::poly::from_ntt+0x1276>
  39eca9:	lea    (%r9,%rbp,1),%r15
  39ecad:	cmp    %r8,%r15
  39ecb0:	jae    39fb35 <valar_spiral_rs::poly::from_ntt+0x1285>
  39ecb6:	mov    0x20(%rsp),%rax
  39ecbb:	mov    (%rax,%rbp,8),%r15
  39ecbf:	mov    0x58(%rsp),%rax
  39ecc4:	mov    (%rax,%rbp,8),%r12
  39ecc8:	mov    0x28(%rsp),%rbx
  39eccd:	sub    %r12,%rbx
  39ecd0:	add    %r15,%rbx
  39ecd3:	mov    %rbx,%rdx
  39ecd6:	mov    0xc8(%rsp),%rax
  39ecde:	mulx   %rax,%rax,%rax
  39ece3:	mulx   0xd0(%rsp),%r13,%rcx
  39eced:	mov    %rax,%rdx
  39ecf0:	mov    0x30(%rsp),%rsi
  39ecf5:	mulx   %rsi,%rdx,%rax
  39ecfa:	sub    %rdx,%r13
  39ecfd:	sbb    %rax,%rcx
  39ed00:	mov    %r13,%r14
  39ed03:	sub    %rsi,%r14
  39ed06:	sbb    $0x0,%rcx
  39ed0a:	setb   %al
  39ed0d:	movzbl %al,%edi
  39ed10:	vzeroupper
  39ed13:	call   39cfc0 <subtle::black_box>
  39ed18:	mov    0xb8(%rsp),%r9
  39ed20:	mov    0xc0(%rsp),%r8
  39ed28:	movzbl %al,%eax
  39ed2b:	mov    %rax,%rcx
  39ed2e:	neg    %rcx
  39ed31:	dec    %rax
  39ed34:	and    %r14,%rax
  39ed37:	and    %r13,%rcx
  39ed3a:	or     %rax,%rcx
  39ed3d:	add    %r15,%r12
  39ed40:	add    %r15,%r15
  39ed43:	cmp    %rbx,%r15
  39ed46:	mov    0x28(%rsp),%rax
  39ed4b:	mov    $0x0,%edx
  39ed50:	cmovb  %rdx,%rax
  39ed54:	sub    %rax,%r12
  39ed57:	test   $0x1,%bl
  39ed5a:	mov    $0x0,%eax
  39ed5f:	cmovne 0x30(%rsp),%rax
  39ed65:	add    %r12,%rax
  39ed68:	shr    $1,%rax
  39ed6b:	mov    0x20(%rsp),%rdx
  39ed70:	mov    %rax,(%rdx,%rbp,8)
  39ed74:	mov    0x58(%rsp),%rax
  39ed79:	mov    %rcx,(%rax,%rbp,8)
  39ed7d:	inc    %rbp
  39ed80:	cmp    %rbp,%r9
  39ed83:	jne    39eca0 <valar_spiral_rs::poly::from_ntt+0x3f0>
  39ed89:	mov    0x80(%rsp),%r15
  39ed91:	mov    0x88(%rsp),%rdx
  39ed99:	cmp    %r15,%rdx
  39ed9c:	mov    %rdx,%rax
  39ed9f:	adc    $0x0,%rax
  39eda3:	mov    0x20(%rsp),%rcx
  39eda8:	add    0x78(%rsp),%rcx
  39edad:	mov    %rcx,0x20(%rsp)
  39edb2:	cmp    %r15,%rdx
  39edb5:	mov    0x18(%rsp),%rbp
  39edba:	mov    0xb0(%rsp),%rbx
  39edc2:	mov    0x40(%rsp),%rdi
  39edc7:	jb     39ec20 <valar_spiral_rs::poly::from_ntt+0x370>
  39edcd:	jmp    39eb60 <valar_spiral_rs::poly::from_ntt+0x2b0>
  39edd2:	data16 data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  39ede0:	xor    %eax,%eax
  39ede2:	data16 data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  39edf0:	lea    (%r15,%rax,1),%rcx
  39edf4:	cmp    %rdi,%rcx
  39edf7:	jae    39fcc9 <valar_spiral_rs::poly::from_ntt+0x1419>
  39edfd:	cmp    0x48(%rsp),%rcx
  39ee02:	jae    39fcdf <valar_spiral_rs::poly::from_ntt+0x142f>
  39ee08:	sub    %r8,0x50(%rsp)
  39ee0d:	jb     39fc8c <valar_spiral_rs::poly::from_ntt+0x13dc>
  39ee13:	inc    %rax
  39ee16:	cmp    %r15,%rax
  39ee19:	jb     39edf0 <valar_spiral_rs::poly::from_ntt+0x540>
  39ee1b:	jmp    39eb60 <valar_spiral_rs::poly::from_ntt+0x2b0>
  39ee20:	mov    0x8(%rsp),%rcx
  39ee25:	mov    0x30(%rcx),%rdi
  39ee29:	mov    0x8(%rcx),%rax
  39ee2d:	mov    %rax,0x70(%rsp)
  39ee32:	mov    0x38(%rcx),%rdx
  39ee36:	mov    %rdx,0x68(%rsp)
  39ee3b:	sub    $0x1,%rdx
  39ee3f:	mov    $0x0,%eax
  39ee44:	cmovb  %rax,%rdx
  39ee48:	mov    %rdx,0x150(%rsp)
  39ee50:	mov    %rdi,%rdx
  39ee53:	shr    $0x2,%rdx
  39ee57:	mov    %edi,%eax
  39ee59:	and    $0x3,%eax
  39ee5c:	cmp    $0x1,%rax
  39ee60:	sbb    $0xffffffffffffffff,%rdx
  39ee64:	mov    %rdx,0x148(%rsp)
  39ee6c:	mov    0x10(%rcx),%rax
  39ee70:	mov    %rax,0xe0(%rsp)
  39ee78:	lea    0x0(,%rdi,8),%rax
  39ee80:	mov    %rax,0x158(%rsp)
  39ee88:	mov    $0x1,%eax
  39ee8d:	mov    0x100(%rsp),%rcx
  39ee95:	mov    %rcx,0x78(%rsp)
  39ee9a:	mov    %rbx,%r8
  39ee9d:	xor    %esi,%esi
  39ee9f:	mov    %rdi,0xa8(%rsp)
  39eea7:	jmp    39eee0 <valar_spiral_rs::poly::from_ntt+0x630>
  39eea9:	nopl   0x0(%rax)
  39eeb0:	mov    0x10(%rsp),%rcx
  39eeb5:	cmp    %rcx,%rsi
  39eeb8:	mov    %rsi,%rax
  39eebb:	adc    $0x0,%rax
  39eebf:	mov    0x158(%rsp),%rdx
  39eec7:	add    %rdx,%r8
  39eeca:	add    %rdx,0x78(%rsp)
  39eecf:	cmp    %rcx,%rsi
  39eed2:	mov    0xb0(%rsp),%rbx
  39eeda:	jae    39f9d0 <valar_spiral_rs::poly::from_ntt+0x1120>
  39eee0:	mov    %rax,%r9
  39eee3:	mov    %rdi,%rax
  39eee6:	imul   %rsi,%rax
  39eeea:	mov    %rax,%rcx
  39eeed:	add    %rdi,%rcx
  39eef0:	jb     39fb61 <valar_spiral_rs::poly::from_ntt+0x12b1>
  39eef6:	mov    0x38(%rsp),%rdx
  39eefb:	cmp    %rdx,%rcx
  39eefe:	ja     39fb74 <valar_spiral_rs::poly::from_ntt+0x12c4>
  39ef04:	mov    %rsi,%r15
  39ef07:	cmp    0xe0(%rsp),%rsi
  39ef0f:	jae    39fd51 <valar_spiral_rs::poly::from_ntt+0x14a1>
  39ef15:	lea    (%r15,%r15,2),%rax
  39ef19:	mov    0x70(%rsp),%rcx
  39ef1e:	mov    0x10(%rcx,%rax,8),%rsi
  39ef23:	cmp    $0x3,%rsi
  39ef27:	jb     39fd8b <valar_spiral_rs::poly::from_ntt+0x14db>
  39ef2d:	mov    %r8,0x80(%rsp)
  39ef35:	je     39fd3a <valar_spiral_rs::poly::from_ntt+0x148a>
  39ef3b:	mov    %r9,0xa0(%rsp)
  39ef43:	cmp    $0x4,%r15
  39ef47:	jae    39fd65 <valar_spiral_rs::poly::from_ntt+0x14b5>
  39ef4d:	mov    0x8(%rsp),%rcx
  39ef52:	mov    0xa8(%rcx,%r15,8),%rcx
  39ef5a:	lea    (%rcx,%rcx,1),%rdx
  39ef5e:	mov    %rdx,0x58(%rsp)
  39ef63:	vmovq  %rdx,%xmm0
  39ef68:	mov    %rcx,0x20(%rsp)
  39ef6d:	vmovq  %rcx,%xmm1
  39ef72:	cmpq   $0x0,0x68(%rsp)
  39ef78:	je     39f440 <valar_spiral_rs::poly::from_ntt+0xb90>
  39ef7e:	mov    0x70(%rsp),%rcx
  39ef83:	lea    (%rcx,%rax,8),%rax
  39ef87:	mov    0x8(%rax),%rax
  39ef8b:	mov    0x38(%rax),%rcx
  39ef8f:	mov    %rcx,0x30(%rsp)
  39ef94:	mov    0x40(%rax),%r8
  39ef98:	mov    0x50(%rax),%rcx
  39ef9c:	mov    %rcx,0x28(%rsp)
  39efa1:	mov    0x58(%rax),%rax
  39efa5:	mov    %rax,0x40(%rsp)
  39efaa:	vpbroadcastq %xmm1,%ymm2
  39efaf:	vpbroadcastq %xmm0,%ymm3
  39efb4:	mov    0x150(%rsp),%rax
  39efbc:	mov    0x68(%rsp),%rcx
  39efc1:	mov    %r8,0xb8(%rsp)
  39efc9:	jmp    39efea <valar_spiral_rs::poly::from_ntt+0x73a>
  39efcb:	nopl   0x0(%rax,%rax,1)
  39efd0:	mov    0x48(%rsp),%rcx
  39efd5:	mov    %rcx,%rax
  39efd8:	sub    $0x1,%rax
  39efdc:	mov    0xa8(%rsp),%rdi
  39efe4:	jb     39f440 <valar_spiral_rs::poly::from_ntt+0xb90>
  39efea:	shrx   %rcx,%rdi,%rbx
  39efef:	mov    %rbx,%r13
  39eff2:	add    %rbx,%r13
  39eff5:	je     39fa8c <valar_spiral_rs::poly::from_ntt+0x11dc>
  39effb:	mov    %rax,%rsi
  39effe:	mov    %rdi,%rax
  39f001:	or     %r13,%rax
  39f004:	shr    $0x20,%rax
  39f008:	je     39f020 <valar_spiral_rs::poly::from_ntt+0x770>
  39f00a:	mov    %rdi,%rax
  39f00d:	xor    %edx,%edx
  39f00f:	div    %r13
  39f012:	jmp    39f027 <valar_spiral_rs::poly::from_ntt+0x777>
  39f014:	data16 data16 cs nopw 0x0(%rax,%rax,1)
  39f020:	mov    %edi,%eax
  39f022:	xor    %edx,%edx
  39f024:	div    %r13d
  39f027:	lea    -0x1(%rcx),%eax
  39f02a:	mov    $0x1,%ecx
  39f02f:	shlx   %rax,%rcx,%rax
  39f034:	mov    %rdi,%rcx
  39f037:	sub    %rdx,%rcx
  39f03a:	mov    %rbx,%r12
  39f03d:	shr    $0x2,%r12
  39f041:	mov    %ebx,%edx
  39f043:	and    $0x3,%edx
  39f046:	cmp    $0x1,%rdx
  39f04a:	sbb    $0xffffffffffffffff,%r12
  39f04e:	cmp    $0x4,%rbx
  39f052:	mov    %rsi,0x48(%rsp)
  39f057:	jae    39f2a0 <valar_spiral_rs::poly::from_ntt+0x9f0>
  39f05d:	test   %rbx,%rbx
  39f060:	je     39f3f2 <valar_spiral_rs::poly::from_ntt+0xb42>
  39f066:	lea    0x1(%rbx),%rdx
  39f06a:	mov    %rdx,0x50(%rsp)
  39f06f:	lea    0x2(%rbx),%rdx
  39f073:	mov    %rdx,0x88(%rsp)
  39f07b:	mov    0x28(%rsp),%rdx
  39f080:	lea    (%rdx,%rax,8),%rdx
  39f084:	mov    %rdx,0xd0(%rsp)
  39f08c:	mov    0x30(%rsp),%rdx
  39f091:	lea    (%rdx,%rax,8),%rdx
  39f095:	mov    %rdx,0xc8(%rsp)
  39f09d:	mov    %rbx,%rdx
  39f0a0:	shl    $0x4,%rdx
  39f0a4:	mov    %rdx,0xc0(%rsp)
  39f0ac:	mov    $0x1,%edi
  39f0b1:	mov    0x78(%rsp),%r11
  39f0b6:	xor    %edx,%edx
  39f0b8:	jmp    39f0e2 <valar_spiral_rs::poly::from_ntt+0x832>
  39f0ba:	nopw   0x0(%rax,%rax,1)
  39f0c0:	lea    0x1(%rdx),%rdi
  39f0c4:	add    0xc0(%rsp),%r11
  39f0cc:	cmp    %rax,%rdx
  39f0cf:	mov    0x18(%rsp),%rbp
  39f0d4:	mov    0xb8(%rsp),%r8
  39f0dc:	jae    39efd0 <valar_spiral_rs::poly::from_ntt+0x720>
  39f0e2:	lea    (%rax,%rdi,1),%r15
  39f0e6:	dec    %r15
  39f0e9:	cmp    %r8,%r15
  39f0ec:	jae    39fbfc <valar_spiral_rs::poly::from_ntt+0x134c>
  39f0f2:	mov    %rdx,%r9
  39f0f5:	mov    %rdi,%rdx
  39f0f8:	mov    0x40(%rsp),%rdi
  39f0fd:	cmp    %rdi,%r15
  39f100:	jae    39fbc4 <valar_spiral_rs::poly::from_ntt+0x1314>
  39f106:	sub    %r13,%rcx
  39f109:	jb     39fb41 <valar_spiral_rs::poly::from_ntt+0x1291>
  39f10f:	mov    0xc8(%rsp),%rsi
  39f117:	mov    -0x8(%rsi,%rdx,8),%rdi
  39f11c:	mov    0xd0(%rsp),%rsi
  39f124:	mov    -0x8(%rsi,%rdx,8),%r9
  39f129:	mov    -0x10(%r11),%r15
  39f12d:	mov    -0x10(%r11,%rbx,8),%r12
  39f132:	mov    0x58(%rsp),%rsi
  39f137:	mov    %rsi,%r14
  39f13a:	sub    %r12,%r14
  39f13d:	add    %r15,%r14
  39f140:	mov    %r14,%r8
  39f143:	imul   %r9,%r8
  39f147:	shr    $0x20,%r8
  39f14b:	mov    %r14,%rbp
  39f14e:	imul   %rdi,%rbp
  39f152:	mov    0x20(%rsp),%r10
  39f157:	imul   %r10,%r8
  39f15b:	sub    %r8,%rbp
  39f15e:	add    %r15,%r12
  39f161:	add    %r15,%r15
  39f164:	cmp    %r14,%r15
  39f167:	mov    $0x0,%r15d
  39f16d:	cmovb  %r15,%rsi
  39f171:	sub    %rsi,%r12
  39f174:	test   $0x1,%r14b
  39f178:	mov    $0x0,%r8d
  39f17e:	cmovne %r10,%r8
  39f182:	add    %r12,%r8
  39f185:	shr    $1,%r8
  39f188:	mov    %r8,-0x10(%r11)
  39f18c:	mov    %rbp,-0x10(%r11,%rbx,8)
  39f191:	cmp    $0x1,%rbx
  39f195:	je     39f0c0 <valar_spiral_rs::poly::from_ntt+0x810>
  39f19b:	cmp    %r13,0x50(%rsp)
  39f1a0:	jae    39fc0b <valar_spiral_rs::poly::from_ntt+0x135b>
  39f1a6:	mov    -0x8(%r11),%r8
  39f1aa:	mov    -0x8(%r11,%rbx,8),%r14
  39f1af:	mov    0x58(%rsp),%rsi
  39f1b4:	mov    %rsi,%r15
  39f1b7:	sub    %r14,%r15
  39f1ba:	add    %r8,%r15
  39f1bd:	mov    %r15,%r12
  39f1c0:	imul   %r9,%r12
  39f1c4:	shr    $0x20,%r12
  39f1c8:	mov    %r15,%rbp
  39f1cb:	imul   %rdi,%rbp
  39f1cf:	mov    0x20(%rsp),%r10
  39f1d4:	imul   %r10,%r12
  39f1d8:	sub    %r12,%rbp
  39f1db:	add    %r8,%r14
  39f1de:	add    %r8,%r8
  39f1e1:	cmp    %r15,%r8
  39f1e4:	mov    %rsi,%r8
  39f1e7:	mov    $0x0,%r12d
  39f1ed:	cmovb  %r12,%r8
  39f1f1:	sub    %r8,%r14
  39f1f4:	test   $0x1,%r15b
  39f1f8:	mov    $0x0,%r8d
  39f1fe:	cmovne %r10,%r8
  39f202:	add    %r14,%r8
  39f205:	shr    $1,%r8
  39f208:	mov    %r8,-0x8(%r11)
  39f20c:	mov    %rbp,-0x8(%r11,%rbx,8)
  39f211:	cmp    $0x2,%rbx
  39f215:	je     39f0c0 <valar_spiral_rs::poly::from_ntt+0x810>
  39f21b:	cmp    %r13,0x88(%rsp)
  39f223:	mov    0x18(%rsp),%rbp
  39f228:	jae    39fc2d <valar_spiral_rs::poly::from_ntt+0x137d>
  39f22e:	mov    (%r11),%r8
  39f231:	mov    (%r11,%rbx,8),%r14
  39f235:	mov    0x58(%rsp),%rsi
  39f23a:	mov    %rsi,%r15
  39f23d:	sub    %r14,%r15
  39f240:	add    %r8,%r15
  39f243:	imul   %r15,%r9
  39f247:	shr    $0x20,%r9
  39f24b:	imul   %r15,%rdi
  39f24f:	mov    0x20(%rsp),%r10
  39f254:	imul   %r10,%r9
  39f258:	sub    %r9,%rdi
  39f25b:	add    %r8,%r14
  39f25e:	add    %r8,%r8
  39f261:	cmp    %r15,%r8
  39f264:	mov    %rsi,%r8
  39f267:	mov    $0x0,%r9d
  39f26d:	cmovb  %r9,%r8
  39f271:	sub    %r8,%r14
  39f274:	test   $0x1,%r15b
  39f278:	mov    $0x0,%r8d
  39f27e:	cmovne %r10,%r8
  39f282:	add    %r14,%r8
  39f285:	shr    $1,%r8
  39f288:	mov    %r8,(%r11)
  39f28b:	mov    %rdi,(%r11,%rbx,8)
  39f28f:	jmp    39f0c0 <valar_spiral_rs::poly::from_ntt+0x810>
  39f294:	data16 data16 cs nopw 0x0(%rax,%rax,1)
  39f2a0:	mov    %rbx,%rsi
  39f2a3:	shl    $0x4,%rsi
  39f2a7:	lea    0x0(,%rbx,8),%r9
  39f2af:	mov    $0x1,%edx
  39f2b4:	mov    0x80(%rsp),%r10
  39f2bc:	xor    %r11d,%r11d
  39f2bf:	nop
  39f2c0:	mov    %r11,%r15
  39f2c3:	add    %rax,%r15
  39f2c6:	cmp    %r8,%r15
  39f2c9:	jae    39fbfc <valar_spiral_rs::poly::from_ntt+0x134c>
  39f2cf:	cmp    0x40(%rsp),%r15
  39f2d4:	jae    39fb95 <valar_spiral_rs::poly::from_ntt+0x12e5>
  39f2da:	sub    %r13,%rcx
  39f2dd:	jb     39fb41 <valar_spiral_rs::poly::from_ntt+0x1291>
  39f2e3:	mov    %rdx,%r11
  39f2e6:	mov    0x28(%rsp),%rdx
  39f2eb:	vpmovzxdq (%rdx,%r15,8),%xmm4
  39f2f1:	vpbroadcastq %xmm4,%ymm4
  39f2f6:	mov    0x30(%rsp),%rdx
  39f2fb:	vmovq  (%rdx,%r15,8),%xmm5
  39f301:	vpmovzxdq %xmm5,%xmm5
  39f306:	vpbroadcastq %xmm5,%ymm5
  39f30b:	lea    (%r10,%r9,1),%rbp
  39f30f:	xor    %edi,%edi
  39f311:	mov    %r12,%rdx
  39f314:	data16 data16 cs nopw 0x0(%rax,%rax,1)
  39f320:	cmp    %r13,%rdi
  39f323:	jae    39facb <valar_spiral_rs::poly::from_ntt+0x121b>
  39f329:	lea    (%rbx,%rdi,1),%r15
  39f32d:	cmp    %r13,%r15
  39f330:	jae    39fabf <valar_spiral_rs::poly::from_ntt+0x120f>
  39f336:	dec    %rdx
  39f339:	vmovdqa (%r10,%rdi,8),%ymm6
  39f33f:	vmovdqa 0x0(%rbp,%rdi,8),%ymm7
  39f345:	vpsubq %ymm7,%ymm3,%ymm8
  39f349:	vpaddq %ymm6,%ymm8,%ymm8
  39f34d:	vpaddq %ymm6,%ymm6,%ymm9
  39f351:	vpcmpgtq %ymm8,%ymm9,%ymm9
  39f356:	vpand  %ymm3,%ymm9,%ymm9
  39f35a:	vpaddq %ymm6,%ymm7,%ymm6
  39f35e:	vpsubq %ymm9,%ymm6,%ymm6
  39f363:	vpmuludq %ymm4,%ymm8,%ymm7
  39f367:	vpsrlq $0x20,%ymm4,%ymm9
  39f36c:	vpmuludq %ymm9,%ymm8,%ymm9
  39f371:	vpsllq $0x20,%ymm9,%ymm9
  39f377:	vpaddq %ymm7,%ymm9,%ymm7
  39f37b:	vpsrlq $0x20,%ymm7,%ymm7
  39f380:	vpsllq $0x3f,%ymm8,%ymm9
  39f386:	vpcmpgtq %ymm9,%ymm11,%ymm9
  39f38b:	vpand  %ymm2,%ymm9,%ymm9
  39f38f:	vpaddq %ymm6,%ymm9,%ymm6
  39f393:	vpsrlq $0x1,%ymm6,%ymm6
  39f398:	vpmuludq %ymm5,%ymm8,%ymm9
  39f39c:	vpsrlq $0x20,%ymm5,%ymm10
  39f3a1:	vpmuludq %ymm10,%ymm8,%ymm8
  39f3a6:	vpsllq $0x20,%ymm8,%ymm8
  39f3ac:	vpaddq %ymm8,%ymm9,%ymm8
  39f3b1:	vpmuludq %ymm2,%ymm7,%ymm7
  39f3b5:	vpsubq %ymm7,%ymm8,%ymm7
  39f3b9:	vmovdqa %ymm6,(%r10,%rdi,8)
  39f3bf:	vmovdqa %ymm7,0x0(%rbp,%rdi,8)
  39f3c5:	add    $0x4,%rdi
  39f3c9:	test   %rdx,%rdx
  39f3cc:	jne    39f320 <valar_spiral_rs::poly::from_ntt+0xa70>
  39f3d2:	cmp    %rax,%r11
  39f3d5:	mov    %r11,%rdx
  39f3d8:	adc    $0x0,%rdx
  39f3dc:	add    %rsi,%r10
  39f3df:	cmp    %rax,%r11
  39f3e2:	mov    0x18(%rsp),%rbp
  39f3e7:	jb     39f2c0 <valar_spiral_rs::poly::from_ntt+0xa10>
  39f3ed:	jmp    39efd0 <valar_spiral_rs::poly::from_ntt+0x720>
  39f3f2:	xor    %edx,%edx
  39f3f4:	data16 data16 cs nopw 0x0(%rax,%rax,1)
  39f400:	lea    (%rax,%rdx,1),%rsi
  39f404:	cmp    %r8,%rsi
  39f407:	jae    39fbf2 <valar_spiral_rs::poly::from_ntt+0x1342>
  39f40d:	mov    0x40(%rsp),%rdi
  39f412:	cmp    %rdi,%rsi
  39f415:	jae    39fbd9 <valar_spiral_rs::poly::from_ntt+0x1329>
  39f41b:	sub    %r13,%rcx
  39f41e:	jb     39fb41 <valar_spiral_rs::poly::from_ntt+0x1291>
  39f424:	inc    %rdx
  39f427:	cmp    %rax,%rdx
  39f42a:	jb     39f400 <valar_spiral_rs::poly::from_ntt+0xb50>
  39f42c:	jmp    39efd0 <valar_spiral_rs::poly::from_ntt+0x720>
  39f431:	data16 data16 data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  39f440:	test   %rdi,%rdi
  39f443:	mov    0x80(%rsp),%r8
  39f44b:	mov    0xa0(%rsp),%rsi
  39f453:	je     39eeb0 <valar_spiral_rs::poly::from_ntt+0x600>
  39f459:	vpbroadcastq %xmm0,%ymm0
  39f45e:	vpbroadcastq %xmm1,%ymm1
  39f463:	xor    %r15d,%r15d
  39f466:	mov    0x148(%rsp),%rax
  39f46e:	xchg   %ax,%ax
  39f470:	cmp    %rdi,%r15
  39f473:	jae    39fcba <valar_spiral_rs::poly::from_ntt+0x140a>
  39f479:	vmovdqa (%r8,%r15,8),%ymm2
  39f47f:	vpcmpgtq %ymm2,%ymm0,%ymm3
  39f484:	vpandn %ymm0,%ymm3,%ymm3
  39f488:	vpsubq %ymm3,%ymm2,%ymm2
  39f48c:	vpcmpgtq %ymm2,%ymm1,%ymm3
  39f491:	vpandn %ymm1,%ymm3,%ymm3
  39f495:	vpsubq %ymm3,%ymm2,%ymm2
  39f499:	vmovdqa %ymm2,(%r8,%r15,8)
  39f49f:	add    $0x4,%r15
  39f4a3:	dec    %rax
  39f4a6:	jne    39f470 <valar_spiral_rs::poly::from_ntt+0xbc0>
  39f4a8:	jmp    39eeb0 <valar_spiral_rs::poly::from_ntt+0x600>
  39f4ad:	mov    0x10(%rsp),%rsi
  39f4b2:	test   %rsi,%rsi
  39f4b5:	je     39f9d0 <valar_spiral_rs::poly::from_ntt+0x1120>
  39f4bb:	cmp    $0x3,%rsi
  39f4bf:	ja     39f51a <valar_spiral_rs::poly::from_ntt+0xc6a>
  39f4c1:	xor    %eax,%eax
  39f4c3:	jmp    39fa15 <valar_spiral_rs::poly::from_ntt+0x1165>
  39f4c8:	test   %r8,%r8
  39f4cb:	mov    0x38(%rsp),%rdx
  39f4d0:	je     39f533 <valar_spiral_rs::poly::from_ntt+0xc83>
  39f4d2:	cmp    %rdx,%r8
  39f4d5:	ja     39fc1f <valar_spiral_rs::poly::from_ntt+0x136f>
  39f4db:	test   %rdi,%rdi
  39f4de:	je     39fd76 <valar_spiral_rs::poly::from_ntt+0x14c6>
  39f4e4:	mov    0x10(%rax),%rsi
  39f4e8:	cmp    $0x3,%rsi
  39f4ec:	jb     39fd8b <valar_spiral_rs::poly::from_ntt+0x14db>
  39f4f2:	mov    0x10(%rsp),%rdi
  39f4f7:	je     39fd3a <valar_spiral_rs::poly::from_ntt+0x148a>
  39f4fd:	mov    0x8(%rsp),%rax
  39f502:	mov    0xa8(%rax),%rax
  39f509:	lea    (%rax,%rax,1),%rcx
  39f50d:	cmp    $0x4,%rdi
  39f511:	jae    39f555 <valar_spiral_rs::poly::from_ntt+0xca5>
  39f513:	xor    %edx,%edx
  39f515:	jmp    39f875 <valar_spiral_rs::poly::from_ntt+0xfc5>
  39f51a:	vmovq  0x28(%rsp),%xmm0
  39f520:	vmovq  0x30(%rsp),%xmm1
  39f526:	cmp    $0x10,%rsi
  39f52a:	jae    39f570 <valar_spiral_rs::poly::from_ntt+0xcc0>
  39f52c:	xor    %eax,%eax
  39f52e:	jmp    39f676 <valar_spiral_rs::poly::from_ntt+0xdc6>
  39f533:	test   %rdi,%rdi
  39f536:	je     39fd76 <valar_spiral_rs::poly::from_ntt+0x14c6>
  39f53c:	mov    0x10(%rax),%rsi
  39f540:	cmp    $0x3,%rsi
  39f544:	jb     39fd8b <valar_spiral_rs::poly::from_ntt+0x14db>
  39f54a:	jne    39f9d0 <valar_spiral_rs::poly::from_ntt+0x1120>
  39f550:	jmp    39fd3a <valar_spiral_rs::poly::from_ntt+0x148a>
  39f555:	vmovq  %rax,%xmm0
  39f55a:	vmovq  %rcx,%xmm1
  39f55f:	cmp    $0x10,%rdi
  39f563:	jae    39f6e2 <valar_spiral_rs::poly::from_ntt+0xe32>
  39f569:	xor    %edx,%edx
  39f56b:	jmp    39f7f2 <valar_spiral_rs::poly::from_ntt+0xf42>
  39f570:	mov    %rsi,%rax
  39f573:	and    $0xfffffffffffffff0,%rax
  39f577:	vpbroadcastq %xmm0,%ymm2
  39f57c:	vpbroadcastq %xmm1,%ymm3
  39f581:	xor    %ecx,%ecx
  39f583:	vmovdqu 0x160(%rsp),%ymm13
  39f58c:	nopl   0x0(%rax)
  39f590:	vmovdqu (%rbx,%rcx,8),%ymm4
  39f595:	vmovdqu 0x20(%rbx,%rcx,8),%ymm5
  39f59b:	vmovdqu 0x40(%rbx,%rcx,8),%ymm6
  39f5a1:	vmovdqu 0x60(%rbx,%rcx,8),%ymm7
  39f5a7:	vpxor  %ymm2,%ymm13,%ymm8
  39f5ab:	vpxor  %ymm4,%ymm13,%ymm9
  39f5af:	vpcmpgtq %ymm9,%ymm8,%ymm9
  39f5b4:	vpandn %ymm2,%ymm9,%ymm9
  39f5b8:	vpxor  %ymm5,%ymm13,%ymm10
  39f5bc:	vpcmpgtq %ymm10,%ymm8,%ymm10
  39f5c1:	vpandn %ymm2,%ymm10,%ymm10
  39f5c5:	vpxor  %ymm6,%ymm13,%ymm11
  39f5c9:	vpcmpgtq %ymm11,%ymm8,%ymm11
  39f5ce:	vpandn %ymm2,%ymm11,%ymm11
  39f5d2:	vpxor  %ymm7,%ymm13,%ymm12
  39f5d6:	vpcmpgtq %ymm12,%ymm8,%ymm8
  39f5db:	vpandn %ymm2,%ymm8,%ymm8
  39f5df:	vpsubq %ymm9,%ymm4,%ymm4
  39f5e4:	vpsubq %ymm10,%ymm5,%ymm5
  39f5e9:	vpsubq %ymm11,%ymm6,%ymm6
  39f5ee:	vpsubq %ymm8,%ymm7,%ymm7
  39f5f3:	vpxor  %ymm4,%ymm13,%ymm8
  39f5f7:	vpxor  %ymm3,%ymm13,%ymm9
  39f5fb:	vpcmpgtq %ymm8,%ymm9,%ymm8
  39f600:	vpandn %ymm3,%ymm8,%ymm8
  39f604:	vpxor  %ymm5,%ymm13,%ymm10
  39f608:	vpcmpgtq %ymm10,%ymm9,%ymm10
  39f60d:	vpandn %ymm3,%ymm10,%ymm10
  39f611:	vpxor  %ymm6,%ymm13,%ymm11
  39f615:	vpcmpgtq %ymm11,%ymm9,%ymm11
  39f61a:	vpandn %ymm3,%ymm11,%ymm11
  39f61e:	vpxor  %ymm7,%ymm13,%ymm12
  39f622:	vpcmpgtq %ymm12,%ymm9,%ymm9
  39f627:	vpandn %ymm3,%ymm9,%ymm9
  39f62b:	vpsubq %ymm8,%ymm4,%ymm4
  39f630:	vpsubq %ymm10,%ymm5,%ymm5
  39f635:	vpsubq %ymm11,%ymm6,%ymm6
  39f63a:	vpsubq %ymm9,%ymm7,%ymm7
  39f63f:	vmovdqu %ymm4,(%rbx,%rcx,8)
  39f644:	vmovdqu %ymm5,0x20(%rbx,%rcx,8)
  39f64a:	vmovdqu %ymm6,0x40(%rbx,%rcx,8)
  39f650:	vmovdqu %ymm7,0x60(%rbx,%rcx,8)
  39f656:	add    $0x10,%rcx
  39f65a:	cmp    %rcx,%rax
  39f65d:	jne    39f590 <valar_spiral_rs::poly::from_ntt+0xce0>
  39f663:	cmp    %rax,%rsi
  39f666:	je     39f9d0 <valar_spiral_rs::poly::from_ntt+0x1120>
  39f66c:	test   $0xc,%sil
  39f670:	je     39fa15 <valar_spiral_rs::poly::from_ntt+0x1165>
  39f676:	mov    %rax,%rcx
  39f679:	mov    %rsi,%rax
  39f67c:	and    $0xfffffffffffffffc,%rax
  39f680:	vpbroadcastq %xmm0,%ymm0
  39f685:	vpbroadcastq %xmm1,%ymm1
  39f68a:	vmovdqu 0x160(%rsp),%ymm5
  39f693:	data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  39f6a0:	vmovdqu (%rbx,%rcx,8),%ymm2
  39f6a5:	vpxor  %ymm5,%ymm0,%ymm3
  39f6a9:	vpxor  %ymm5,%ymm2,%ymm4
  39f6ad:	vpcmpgtq %ymm4,%ymm3,%ymm3
  39f6b2:	vpandn %ymm0,%ymm3,%ymm3
  39f6b6:	vpsubq %ymm3,%ymm2,%ymm2
  39f6ba:	vpxor  %ymm5,%ymm2,%ymm3
  39f6be:	vpxor  %ymm5,%ymm1,%ymm4
  39f6c2:	vpcmpgtq %ymm3,%ymm4,%ymm3
  39f6c7:	vpandn %ymm1,%ymm3,%ymm3
  39f6cb:	vpsubq %ymm3,%ymm2,%ymm2
  39f6cf:	vmovdqu %ymm2,(%rbx,%rcx,8)
  39f6d4:	add    $0x4,%rcx
  39f6d8:	cmp    %rcx,%rax
  39f6db:	jne    39f6a0 <valar_spiral_rs::poly::from_ntt+0xdf0>
  39f6dd:	jmp    39fa10 <valar_spiral_rs::poly::from_ntt+0x1160>
  39f6e2:	mov    %rdi,%rdx
  39f6e5:	and    $0xfffffffffffffff0,%rdx
  39f6e9:	vpbroadcastq %xmm0,%ymm2
  39f6ee:	vpbroadcastq %xmm1,%ymm3
  39f6f3:	vmovdqu 0x160(%rsp),%ymm14
  39f6fc:	vpxor  %ymm3,%ymm14,%ymm4
  39f700:	vpxor  %ymm2,%ymm14,%ymm5
  39f704:	xor    %esi,%esi
  39f706:	cs nopw 0x0(%rax,%rax,1)
  39f710:	vmovdqu (%rbx,%rsi,8),%ymm6
  39f715:	vmovdqu 0x20(%rbx,%rsi,8),%ymm7
  39f71b:	vmovdqu 0x40(%rbx,%rsi,8),%ymm8
  39f721:	vmovdqu 0x60(%rbx,%rsi,8),%ymm9
  39f727:	vpxor  %ymm6,%ymm14,%ymm10
  39f72b:	vpcmpgtq %ymm10,%ymm4,%ymm10
  39f730:	vpandn %ymm3,%ymm10,%ymm10
  39f734:	vpxor  %ymm7,%ymm14,%ymm11
  39f738:	vpcmpgtq %ymm11,%ymm4,%ymm11
  39f73d:	vpandn %ymm3,%ymm11,%ymm11
  39f741:	vpxor  %ymm14,%ymm8,%ymm12
  39f746:	vpcmpgtq %ymm12,%ymm4,%ymm12
  39f74b:	vpandn %ymm3,%ymm12,%ymm12
  39f74f:	vpxor  %ymm14,%ymm9,%ymm13
  39f754:	vpcmpgtq %ymm13,%ymm4,%ymm13
  39f759:	vpandn %ymm3,%ymm13,%ymm13
  39f75d:	vpsubq %ymm10,%ymm6,%ymm6
  39f762:	vpsubq %ymm11,%ymm7,%ymm7
  39f767:	vpsubq %ymm12,%ymm8,%ymm8
  39f76c:	vpsubq %ymm13,%ymm9,%ymm9
  39f771:	vpxor  %ymm6,%ymm14,%ymm10
  39f775:	vpcmpgtq %ymm10,%ymm5,%ymm10
  39f77a:	vpandn %ymm2,%ymm10,%ymm10
  39f77e:	vpxor  %ymm7,%ymm14,%ymm11
  39f782:	vpcmpgtq %ymm11,%ymm5,%ymm11
  39f787:	vpandn %ymm2,%ymm11,%ymm11
  39f78b:	vpxor  %ymm14,%ymm8,%ymm12
  39f790:	vpcmpgtq %ymm12,%ymm5,%ymm12
  39f795:	vpandn %ymm2,%ymm12,%ymm12
  39f799:	vpxor  %ymm14,%ymm9,%ymm13
  39f79e:	vpcmpgtq %ymm13,%ymm5,%ymm13
  39f7a3:	vpandn %ymm2,%ymm13,%ymm13
  39f7a7:	vpsubq %ymm10,%ymm6,%ymm6
  39f7ac:	vpsubq %ymm11,%ymm7,%ymm7
  39f7b1:	vpsubq %ymm12,%ymm8,%ymm8
  39f7b6:	vpsubq %ymm13,%ymm9,%ymm9
  39f7bb:	vmovdqu %ymm6,(%rbx,%rsi,8)
  39f7c0:	vmovdqu %ymm7,0x20(%rbx,%rsi,8)
  39f7c6:	vmovdqu %ymm8,0x40(%rbx,%rsi,8)
  39f7cc:	vmovdqu %ymm9,0x60(%rbx,%rsi,8)
  39f7d2:	add    $0x10,%rsi
  39f7d6:	cmp    %rsi,%rdx
  39f7d9:	jne    39f710 <valar_spiral_rs::poly::from_ntt+0xe60>
  39f7df:	cmp    %rdx,%rdi
  39f7e2:	je     39f9d0 <valar_spiral_rs::poly::from_ntt+0x1120>
  39f7e8:	test   $0xc,%dil
  39f7ec:	je     39f875 <valar_spiral_rs::poly::from_ntt+0xfc5>
  39f7f2:	mov    %rdx,%rsi
  39f7f5:	mov    %rdi,%rdx
  39f7f8:	and    $0xfffffffffffffffc,%rdx
  39f7fc:	vpbroadcastq %xmm0,%ymm0
  39f801:	vpbroadcastq %xmm1,%ymm1
  39f806:	vmovdqu 0x160(%rsp),%ymm6
  39f80f:	vpxor  %ymm6,%ymm1,%ymm2
  39f813:	vpxor  %ymm6,%ymm0,%ymm3
  39f817:	nopw   0x0(%rax,%rax,1)
  39f820:	vmovdqu (%rbx,%rsi,8),%ymm4
  39f825:	vpxor  %ymm6,%ymm4,%ymm5
  39f829:	vpcmpgtq %ymm5,%ymm2,%ymm5
  39f82e:	vpandn %ymm1,%ymm5,%ymm5
  39f832:	vpsubq %ymm5,%ymm4,%ymm4
  39f836:	vpxor  %ymm6,%ymm4,%ymm5
  39f83a:	vpcmpgtq %ymm5,%ymm3,%ymm5
  39f83f:	vpandn %ymm0,%ymm5,%ymm5
  39f843:	vpsubq %ymm5,%ymm4,%ymm4
  39f847:	vmovdqu %ymm4,(%rbx,%rsi,8)
  39f84c:	add    $0x4,%rsi
  39f850:	cmp    %rsi,%rdx
  39f853:	jne    39f820 <valar_spiral_rs::poly::from_ntt+0xf70>
  39f855:	cmp    %rdx,%rdi
  39f858:	jne    39f875 <valar_spiral_rs::poly::from_ntt+0xfc5>
  39f85a:	jmp    39f9d0 <valar_spiral_rs::poly::from_ntt+0x1120>
  39f85f:	nop
  39f860:	sub    %rdi,%rsi
  39f863:	mov    %rsi,(%rbx,%rdx,8)
  39f867:	inc    %rdx
  39f86a:	cmp    %rdx,0x10(%rsp)
  39f86f:	je     39f9d0 <valar_spiral_rs::poly::from_ntt+0x1120>
  39f875:	mov    (%rbx,%rdx,8),%rsi
  39f879:	mov    $0x0,%edi
  39f87e:	cmp    %rcx,%rsi
  39f881:	jb     39f886 <valar_spiral_rs::poly::from_ntt+0xfd6>
  39f883:	mov    %rcx,%rdi
  39f886:	sub    %rdi,%rsi
  39f889:	mov    $0x0,%edi
  39f88e:	cmp    %rax,%rsi
  39f891:	jb     39f860 <valar_spiral_rs::poly::from_ntt+0xfb0>
  39f893:	mov    %rax,%rdi
  39f896:	jmp    39f860 <valar_spiral_rs::poly::from_ntt+0xfb0>
  39f898:	nopl   0x0(%rax,%rax,1)
  39f8a0:	mov    0x90(%rsp),%rax
  39f8a8:	mov    (%rax),%rax
  39f8ab:	cmp    $0x1,%rax
  39f8af:	jne    39f8d0 <valar_spiral_rs::poly::from_ntt+0x1020>
  39f8b1:	cmp    %rcx,%r9
  39f8b4:	jae    39fd10 <valar_spiral_rs::poly::from_ntt+0x1460>
  39f8ba:	mov    (%rbx,%r9,8),%rax
  39f8be:	jmp    39f997 <valar_spiral_rs::poly::from_ntt+0x10e7>
  39f8c3:	data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  39f8d0:	cmp    %rcx,%r9
  39f8d3:	jae    39fd0b <valar_spiral_rs::poly::from_ntt+0x145b>
  39f8d9:	mov    0x8(%rsp),%rdx
  39f8de:	mov    0x30(%rdx),%rdi
  39f8e2:	add    %r9,%rdi
  39f8e5:	cmp    %rcx,%rdi
  39f8e8:	jae    39fd1c <valar_spiral_rs::poly::from_ntt+0x146c>
  39f8ee:	cmp    $0x2,%rax
  39f8f2:	jne    39faed <valar_spiral_rs::poly::from_ntt+0x123d>
  39f8f8:	mov    (%rbx,%r9,8),%rdx
  39f8fc:	mov    (%rbx,%rdi,8),%rax
  39f900:	mov    0x8(%rsp),%r11
  39f905:	mulx   0xa0(%r11),%r10,%rdi
  39f90e:	mov    %rax,%rdx
  39f911:	mulx   0x98(%r11),%rax,%rcx
  39f91a:	add    %r10,%rax
  39f91d:	adc    %rdi,%rcx
  39f920:	mov    0x88(%r11),%r14
  39f927:	mov    0x90(%r11),%rdi
  39f92e:	mov    %rax,%rdx
  39f931:	mulx   %r14,%r11,%r11
  39f936:	mulx   %rdi,%rbx,%r10
  39f93b:	mov    %rcx,%rdx
  39f93e:	mulx   %r14,%r15,%r14
  39f943:	mov    $0x0,%edx
  39f948:	add    %rbx,%r11
  39f94b:	jb     39f958 <valar_spiral_rs::poly::from_ntt+0x10a8>
  39f94d:	mov    %r11,%r12
  39f950:	not    %r12
  39f953:	cmp    %r15,%r12
  39f956:	jb     39f9ba <valar_spiral_rs::poly::from_ntt+0x110a>
  39f958:	mov    0x8(%rsp),%r15
  39f95d:	mov    0xc8(%r15),%r15
  39f964:	imul   %rcx,%rdi
  39f968:	add    %r14,%rdi
  39f96b:	cmp    %rbx,%r11
  39f96e:	adc    %r10,%rdi
  39f971:	add    %rdx,%rdi
  39f974:	imul   %r15,%rdi
  39f978:	sub    %rdi,%rax
  39f97b:	cmp    %r15,%rax
  39f97e:	mov    $0x0,%ecx
  39f983:	cmovae %r15,%rcx
  39f987:	sub    %rcx,%rax
  39f98a:	mov    0x38(%rsp),%rcx
  39f98f:	mov    0xb0(%rsp),%rbx
  39f997:	cmp    %r9,0x98(%rsp)
  39f99f:	je     39fcf7 <valar_spiral_rs::poly::from_ntt+0x1447>
  39f9a5:	mov    %rax,(%r8,%r9,8)
  39f9a9:	inc    %r9
  39f9ac:	cmp    %r9,%rsi
  39f9af:	jne    39f8a0 <valar_spiral_rs::poly::from_ntt+0xff0>
  39f9b5:	jmp    39e9f0 <valar_spiral_rs::poly::from_ntt+0x140>
  39f9ba:	mov    $0x1,%edx
  39f9bf:	jmp    39f958 <valar_spiral_rs::poly::from_ntt+0x10a8>
  39f9c1:	data16 data16 data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  39f9d0:	mov    0x8(%rsp),%rax
  39f9d5:	mov    0x30(%rax),%rsi
  39f9d9:	test   %rsi,%rsi
  39f9dc:	mov    0x38(%rsp),%rcx
  39f9e1:	je     39e9f0 <valar_spiral_rs::poly::from_ntt+0x140>
  39f9e7:	mov    0x98(%rsp),%r8
  39f9ef:	imul   0xd8(%rsp),%r8
  39f9f8:	add    0x108(%rsp),%r8
  39fa00:	xor    %r9d,%r9d
  39fa03:	jmp    39f8a0 <valar_spiral_rs::poly::from_ntt+0xff0>
  39fa08:	nopl   0x0(%rax,%rax,1)
  39fa10:	cmp    %rax,%rsi
  39fa13:	je     39f9d0 <valar_spiral_rs::poly::from_ntt+0x1120>
  39fa15:	mov    (%rbx,%rax,8),%rcx
  39fa19:	mov    $0x0,%edx
  39fa1e:	cmp    0x28(%rsp),%rcx
  39fa23:	jb     39fa2a <valar_spiral_rs::poly::from_ntt+0x117a>
  39fa25:	mov    0x28(%rsp),%rdx
  39fa2a:	sub    %rdx,%rcx
  39fa2d:	mov    $0x0,%edx
  39fa32:	cmp    0x30(%rsp),%rcx
  39fa37:	jb     39fa3e <valar_spiral_rs::poly::from_ntt+0x118e>
  39fa39:	mov    0x30(%rsp),%rdx
  39fa3e:	sub    %rdx,%rcx
  39fa41:	mov    %rcx,(%rbx,%rax,8)
  39fa45:	inc    %rax
  39fa48:	jmp    39fa10 <valar_spiral_rs::poly::from_ntt+0x1160>
  39fa4a:	mov    0xf8(%rsp),%rax
  39fa52:	add    0xe8(%rsp),%rax
  39fa5a:	cmp    0xf0(%rsp),%rdi
  39fa62:	jne    39e9bf <valar_spiral_rs::poly::from_ntt+0x10f>
  39fa68:	mov    0x0(%rbp),%rax
  39fa6c:	inc    %rax
  39fa6f:	jmp    39fa73 <valar_spiral_rs::poly::from_ntt+0x11c3>
  39fa71:	xor    %eax,%eax
  39fa73:	mov    %rax,0x0(%rbp)
  39fa77:	add    $0x1b8,%rsp
  39fa7e:	pop    %rbx
  39fa7f:	pop    %r12
  39fa81:	pop    %r13
  39fa83:	pop    %r14
  39fa85:	pop    %r15
  39fa87:	pop    %rbp
  39fa88:	vzeroupper
  39fa8b:	ret
  39fa8c:	lea    0x1d3ad(%rip),%rsi        # 3bce40 <tokio::runtime::task::waker::WAKER_VTABLE+0x1148>
  39fa93:	lea    0x188(%rsp),%rdi
  39fa9b:	lea    0x1d75e(%rip),%rax        # 3bd200 <tokio::runtime::task::waker::WAKER_VTABLE+0x1508>
  39faa2:	mov    %rax,(%rdi)
  39faa5:	vmovdqa -0x35e24d(%rip),%ymm0        # 41860 <GCC_except_table4170+0x77c>
  39faad:	vmovdqu %ymm0,0x8(%rdi)
  39fab2:	vzeroupper
  39fab5:	call   1a10e0 <core::panicking::panic_fmt>
  39faba:	jmp    39fd38 <valar_spiral_rs::poly::from_ntt+0x1488>
  39fabf:	lea    0x1d3f2(%rip),%rdx        # 3bceb8 <tokio::runtime::task::waker::WAKER_VTABLE+0x11c0>
  39fac6:	mov    %r13,%rsi
  39fac9:	jmp    39fad8 <valar_spiral_rs::poly::from_ntt+0x1228>
  39facb:	mov    %rdi,%r15
  39face:	mov    %r13,%rsi
  39fad1:	lea    0x1d3c8(%rip),%rdx        # 3bcea0 <tokio::runtime::task::waker::WAKER_VTABLE+0x11a8>
  39fad8:	mov    0x18(%rsp),%rbp
  39fadd:	mov    %r15,%rdi
  39fae0:	vzeroupper
  39fae3:	call   1a4b60 <core::panicking::panic_bounds_check>
  39fae8:	jmp    39fd38 <valar_spiral_rs::poly::from_ntt+0x1488>
  39faed:	movq   $0x0,0x188(%rsp)
  39faf9:	lea    -0x35c860(%rip),%rdx        # 432a0 <GCC_except_table4170+0x21bc>
  39fb00:	lea    0x1d751(%rip),%r8        # 3bd258 <tokio::runtime::task::waker::WAKER_VTABLE+0x1560>
  39fb07:	lea    0x188(%rsp),%rcx
  39fb0f:	xor    %edi,%edi
  39fb11:	mov    0x90(%rsp),%rsi
  39fb19:	vzeroupper
  39fb1c:	call   1a8ffa <core::panicking::assert_failed>
  39fb21:	jmp    39fd38 <valar_spiral_rs::poly::from_ntt+0x1488>
  39fb26:	mov    %rbp,%r15
  39fb29:	mov    %r8,%rsi
  39fb2c:	lea    0x1d265(%rip),%rdx        # 3bcd98 <tokio::runtime::task::waker::WAKER_VTABLE+0x10a0>
  39fb33:	jmp    39fad8 <valar_spiral_rs::poly::from_ntt+0x1228>
  39fb35:	lea    0x1d274(%rip),%rdx        # 3bcdb0 <tokio::runtime::task::waker::WAKER_VTABLE+0x10b8>
  39fb3c:	mov    %r8,%rsi
  39fb3f:	jmp    39fad8 <valar_spiral_rs::poly::from_ntt+0x1228>
  39fb41:	lea    0x1d340(%rip),%rdi        # 3bce88 <tokio::runtime::task::waker::WAKER_VTABLE+0x1190>
  39fb48:	vzeroupper
  39fb4b:	call   1a1100 <core::option::unwrap_failed>
  39fb50:	jmp    39fd38 <valar_spiral_rs::poly::from_ntt+0x1488>
  39fb55:	lea    0x1d1dc(%rip),%rsi        # 3bcd38 <tokio::runtime::task::waker::WAKER_VTABLE+0x1040>
  39fb5c:	jmp    39fa93 <valar_spiral_rs::poly::from_ntt+0x11e3>
  39fb61:	mov    %rcx,0x10(%rsp)
  39fb66:	lea    0x1d37b(%rip),%rcx        # 3bcee8 <tokio::runtime::task::waker::WAKER_VTABLE+0x11f0>
  39fb6d:	mov    0x38(%rsp),%rdx
  39fb72:	jmp    39fb80 <valar_spiral_rs::poly::from_ntt+0x12d0>
  39fb74:	mov    %rcx,0x10(%rsp)
  39fb79:	lea    0x1d368(%rip),%rcx        # 3bcee8 <tokio::runtime::task::waker::WAKER_VTABLE+0x11f0>
  39fb80:	mov    %rax,%rdi
  39fb83:	mov    0x10(%rsp),%rsi
  39fb88:	vzeroupper
  39fb8b:	call   1a23c0 <core::slice::index::slice_index_fail>
  39fb90:	jmp    39fd38 <valar_spiral_rs::poly::from_ntt+0x1488>
  39fb95:	lea    0x1d2d4(%rip),%rdx        # 3bce70 <tokio::runtime::task::waker::WAKER_VTABLE+0x1178>
  39fb9c:	mov    0x40(%rsp),%rsi
  39fba1:	jmp    39fadd <valar_spiral_rs::poly::from_ntt+0x122d>
  39fba6:	lea    0x1d54b(%rip),%rcx        # 3bd0f8 <tokio::runtime::task::waker::WAKER_VTABLE+0x1400>
  39fbad:	xor    %edi,%edi
  39fbaf:	mov    %rdx,%rsi
  39fbb2:	mov    0x38(%rsp),%rdx
  39fbb7:	vzeroupper
  39fbba:	call   1a23c0 <core::slice::index::slice_index_fail>
  39fbbf:	jmp    39fd38 <valar_spiral_rs::poly::from_ntt+0x1488>
  39fbc4:	add    %rax,%r9
  39fbc7:	mov    %rdi,%rsi
  39fbca:	mov    %r9,%r15
  39fbcd:	lea    0x1d29c(%rip),%rdx        # 3bce70 <tokio::runtime::task::waker::WAKER_VTABLE+0x1178>
  39fbd4:	jmp    39fadd <valar_spiral_rs::poly::from_ntt+0x122d>
  39fbd9:	cmp    %rax,%rdi
  39fbdc:	cmova  %rdi,%rax
  39fbe0:	mov    %rdi,%rsi
  39fbe3:	mov    %rax,%r15
  39fbe6:	lea    0x1d283(%rip),%rdx        # 3bce70 <tokio::runtime::task::waker::WAKER_VTABLE+0x1178>
  39fbed:	jmp    39fadd <valar_spiral_rs::poly::from_ntt+0x122d>
  39fbf2:	cmp    %rax,%r8
  39fbf5:	cmova  %r8,%rax
  39fbf9:	mov    %rax,%r15
  39fbfc:	mov    %r8,%rsi
  39fbff:	lea    0x1d252(%rip),%rdx        # 3bce58 <tokio::runtime::task::waker::WAKER_VTABLE+0x1160>
  39fc06:	jmp    39fadd <valar_spiral_rs::poly::from_ntt+0x122d>
  39fc0b:	mov    0x50(%rsp),%r15
  39fc10:	mov    %r13,%rsi
  39fc13:	lea    0x1d2b6(%rip),%rdx        # 3bced0 <tokio::runtime::task::waker::WAKER_VTABLE+0x11d8>
  39fc1a:	jmp    39fad8 <valar_spiral_rs::poly::from_ntt+0x1228>
  39fc1f:	xor    %eax,%eax
  39fc21:	lea    0x1d1a0(%rip),%rcx        # 3bcdc8 <tokio::runtime::task::waker::WAKER_VTABLE+0x10d0>
  39fc28:	jmp    39fb80 <valar_spiral_rs::poly::from_ntt+0x12d0>
  39fc2d:	mov    0x88(%rsp),%r15
  39fc35:	mov    %r13,%rsi
  39fc38:	lea    0x1d291(%rip),%rdx        # 3bced0 <tokio::runtime::task::waker::WAKER_VTABLE+0x11d8>
  39fc3f:	jmp    39fadd <valar_spiral_rs::poly::from_ntt+0x122d>
  39fc44:	mov    %fs:0x0,%rax
  39fc4d:	lea    -0x70(%rax),%rax
  39fc54:	mov    %rdi,%rbx
  39fc57:	mov    %rax,%rdi
  39fc5a:	mov    %rsi,%r14
  39fc5d:	call   39fdc0 <std::sys::thread_local::native::lazy::Storage<T,D>::get_or_init_slow>
  39fc62:	mov    %r14,%rsi
  39fc65:	mov    %rbx,%rdi
  39fc68:	mov    %rax,%rbp
  39fc6b:	test   %rax,%rax
  39fc6e:	jne    39e8e9 <valar_spiral_rs::poly::from_ntt+0x39>
  39fc74:	lea    0x1d69d(%rip),%rdi        # 3bd318 <tokio::runtime::task::waker::WAKER_VTABLE+0x1620>
  39fc7b:	call   294650 <std::thread::local::panic_access_error>
  39fc80:	lea    0x1d489(%rip),%rdi        # 3bd110 <tokio::runtime::task::waker::WAKER_VTABLE+0x1418>
  39fc87:	call   1ab5e0 <core::cell::panic_already_borrowed>
  39fc8c:	lea    0x1d0ed(%rip),%rdi        # 3bcd80 <tokio::runtime::task::waker::WAKER_VTABLE+0x1088>
  39fc93:	jmp    39fb48 <valar_spiral_rs::poly::from_ntt+0x1298>
  39fc98:	mov    0x48(%rsp),%rsi
  39fc9d:	lea    0x1d0c4(%rip),%rdx        # 3bcd68 <tokio::runtime::task::waker::WAKER_VTABLE+0x1070>
  39fca4:	jmp    39fadd <valar_spiral_rs::poly::from_ntt+0x122d>
  39fca9:	lea    0x1d0a0(%rip),%rdx        # 3bcd50 <tokio::runtime::task::waker::WAKER_VTABLE+0x1058>
  39fcb0:	mov    0x40(%rsp),%rsi
  39fcb5:	jmp    39fadd <valar_spiral_rs::poly::from_ntt+0x122d>
  39fcba:	mov    %rdi,%rsi
  39fcbd:	lea    0x1d164(%rip),%rdx        # 3bce28 <tokio::runtime::task::waker::WAKER_VTABLE+0x1130>
  39fcc4:	jmp    39fadd <valar_spiral_rs::poly::from_ntt+0x122d>
  39fcc9:	cmp    %r15,%rdi
  39fccc:	cmova  %rdi,%r15
  39fcd0:	mov    %rdi,%rsi
  39fcd3:	lea    0x1d076(%rip),%rdx        # 3bcd50 <tokio::runtime::task::waker::WAKER_VTABLE+0x1058>
  39fcda:	jmp    39fadd <valar_spiral_rs::poly::from_ntt+0x122d>
  39fcdf:	mov    0x48(%rsp),%rsi
  39fce4:	cmp    %r15,%rsi
  39fce7:	cmova  %rsi,%r15
  39fceb:	lea    0x1d076(%rip),%rdx        # 3bcd68 <tokio::runtime::task::waker::WAKER_VTABLE+0x1070>
  39fcf2:	jmp    39fadd <valar_spiral_rs::poly::from_ntt+0x122d>
  39fcf7:	mov    0x98(%rsp),%rcx
  39fcff:	mov    %rcx,%rdi
  39fd02:	lea    0x1d3d7(%rip),%rax        # 3bd0e0 <tokio::runtime::task::waker::WAKER_VTABLE+0x13e8>
  39fd09:	jmp    39fd23 <valar_spiral_rs::poly::from_ntt+0x1473>
  39fd0b:	mov    %r9,%rdi
  39fd0e:	jmp    39fd28 <valar_spiral_rs::poly::from_ntt+0x1478>
  39fd10:	mov    %r9,%rdi
  39fd13:	lea    0x1d4f6(%rip),%rax        # 3bd210 <tokio::runtime::task::waker::WAKER_VTABLE+0x1518>
  39fd1a:	jmp    39fd23 <valar_spiral_rs::poly::from_ntt+0x1473>
  39fd1c:	lea    0x1d51d(%rip),%rax        # 3bd240 <tokio::runtime::task::waker::WAKER_VTABLE+0x1548>
  39fd23:	mov    %rax,0x60(%rsp)
  39fd28:	mov    %rcx,%rsi
  39fd2b:	mov    0x60(%rsp),%rdx
  39fd30:	vzeroupper
  39fd33:	call   1a4b60 <core::panicking::panic_bounds_check>
  39fd38:	ud2
  39fd3a:	mov    $0x3,%r15d
  39fd40:	mov    $0x3,%esi
  39fd45:	lea    0x1d59c(%rip),%rdx        # 3bd2e8 <tokio::runtime::task::waker::WAKER_VTABLE+0x15f0>
  39fd4c:	jmp    39fadd <valar_spiral_rs::poly::from_ntt+0x122d>
  39fd51:	mov    0xe0(%rsp),%rsi
  39fd59:	lea    0x1d540(%rip),%rdx        # 3bd2a0 <tokio::runtime::task::waker::WAKER_VTABLE+0x15a8>
  39fd60:	jmp    39fadd <valar_spiral_rs::poly::from_ntt+0x122d>
  39fd65:	mov    $0x4,%esi
  39fd6a:	lea    0x1d09f(%rip),%rdx        # 3bce10 <tokio::runtime::task::waker::WAKER_VTABLE+0x1118>
  39fd71:	jmp    39fadd <valar_spiral_rs::poly::from_ntt+0x122d>
  39fd76:	mov    %rdi,%rsi
  39fd79:	xor    %r15d,%r15d
  39fd7c:	lea    0x1d51d(%rip),%rdx        # 3bd2a0 <tokio::runtime::task::waker::WAKER_VTABLE+0x15a8>
  39fd83:	jmp    39fadd <valar_spiral_rs::poly::from_ntt+0x122d>
  39fd88:	mov    %rcx,%rsi
  39fd8b:	mov    $0x2,%r15d
  39fd91:	lea    0x1d520(%rip),%rdx        # 3bd2b8 <tokio::runtime::task::waker::WAKER_VTABLE+0x15c0>
  39fd98:	jmp    39fadd <valar_spiral_rs::poly::from_ntt+0x122d>
  39fd9d:	incq   0x0(%rbp)
  39fda1:	mov    %rax,%rdi
  39fda4:	call   3a2da0 <_Unwind_Resume@plt>
  39fda9:	incq   0x0(%rbp)
  39fdad:	mov    %rax,%rdi
  39fdb0:	call   3a2da0 <_Unwind_Resume@plt>
